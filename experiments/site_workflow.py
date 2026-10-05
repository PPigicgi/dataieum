"""Stateless LangGraph orchestration inside the existing request harness.

No checkpoint store, background runs, remote tracing, model retries or answer
cache. Runtime context belongs to one invocation; the compiled graph is shared.
"""
from dataclasses import dataclass, field
import asyncio
from contextlib import asynccontextmanager
import copy
import time
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.errors import GraphRecursionError
from langgraph.runtime import Runtime
from langsmith import tracing_context

from discovery_harness.errors import AdapterContractError, BudgetExceeded
from discovery_harness.resources import bounded_json
from discovery_harness.site_search import merge_plan, validate_plan, validate_site_result


class State(TypedDict, total=False):
    query: str
    previous: dict | None
    draft: dict
    plan: dict
    traversal: list
    result: dict
    embedding: dict
    search: dict


@dataclass
class RequestContext:
    session: Any
    stages: list = field(default_factory=list)
    metadata_lease: Any = None


def expand_vector_plan(plan):
    """Reuse curated purpose -> analysis -> indicator definitions before retrieval."""
    if plan['question'] or plan.get('need_selection') != 'workflow':
        return plan
    from discovery_harness.ontology_definitions import definitions
    from discovery_harness.site_search import node_plan
    result = copy.deepcopy(plan)
    prior = {need['indicator']: need for need in result['needs']}
    workflows = {item['id']: item for item in definitions()['workflows']}
    inferred = {}
    for identifier in result['workflow_ids']:
        if identifier not in workflows:
            raise ValueError('Unknown purpose workflow')
        for analysis in workflows[identifier]['analyses']:
            for indicator in analysis['indicators']:
                inferred.setdefault(indicator['id'], indicator['reason'])
    if not inferred:
        result.update(needs=[], question='데이터를 어떤 업무에 활용하려고 하나요?')
    elif len(inferred) > 5:
        result.update(needs=[], question='필요한 데이터 종류가 5개를 넘어요. 어떤 업무를 먼저 확인할까요?')
    else:
        result['needs'] = []
        for identifier, reason in inferred.items():
            need = copy.deepcopy(prior.get(identifier) or node_plan(identifier)['needs'][0])
            need['reason'] = reason[:90]
            result['needs'].append(need)
    return validate_plan(result)


class SiteWorkflow:
    recursion_limit = 9  # Graph supersteps, separate from LLM/tool/time budgets.

    def __init__(self, sites, interpret, *, vector_tools=None, judge=None, topic_lookup=None):
        self.sites, self.interpret = sites, interpret
        self.vector_tools, self.judge = vector_tools, judge
        self.topic_lookup = topic_lookup
        # At most 25 sessions retain a <=512KiB search response while composing.
        # Interpreting/embedding other questions does not consume this permit.
        self.metadata_slots=asyncio.Semaphore(25)
        self.metadata_active=self.metadata_waiters=self.metadata_peak=0
        graph = StateGraph(State, context_schema=RequestContext)

        def node(name, operation):
            async def measured(state: State, runtime: Runtime[RequestContext]):
                started = time.monotonic()
                try:
                    if name=='cosine_search' and runtime.context.metadata_lease is None:
                        slot=self.metadata_slot(runtime.context.session)
                        self.metadata_waiters+=1
                        try:await slot.__aenter__()
                        finally:self.metadata_waiters-=1
                        runtime.context.metadata_lease=slot
                        self.metadata_active+=1
                        self.metadata_peak=max(self.metadata_peak,self.metadata_active)
                    return await operation(state, runtime.context.session)
                finally:
                    runtime.context.stages.append({'node': name, 'milliseconds':
                        round((time.monotonic() - started) * 1000, 3)})
            graph.add_node(name, measured)

        async def interpret_intent(state, session):
            return {'draft': await self.interpret(session, state['query'], state.get('previous'))}

        async def normalize(state, session):
            try:
                plan = (merge_plan(state.get('previous'), state['draft'], query=state['query'])
                        if 'query' in state else validate_plan(state['plan']))
                normalized = self.sites.normalize(plan)
                if vector_tools:
                    normalized = expand_vector_plan(normalized)
                    if normalized['needs'] and not normalized['question']:
                        from discovery_harness.vector_tools import query_text
                        try: normalized['semantic_query'] = query_text('', normalized)
                        except AdapterContractError:
                            normalized['question'] = '찾으려는 자료의 주제나 지표를 조금 더 구체적으로 알려 주세요.'
                return {'plan': normalized}
            except ValueError as error:
                raise AdapterContractError('Invalid site intent') from error

        async def clarify(state, session):
            return {'result': self.sites.recommend(state['plan'], candidates={})}

        async def traverse(state, session):
            return {'traversal': await session.tool(
                lambda: self.sites.traverse_graph(session, state['plan']))}

        async def evidence(state, session):
            return {'result': self.sites.compose_graph(*state['traversal'])}

        def request_text(state):
            from discovery_harness.site_search import INDICATORS
            # Node selection is explicit even when no business purpose was given.
            return (state.get('query') or state['plan']['purpose'] or
                    ' · '.join(INDICATORS[need['indicator']] for need in state['plan']['needs']) + ' 자료 검색')

        async def embed(state, session):
            from discovery_harness.vector_tools import query_text
            text = query_text('',state['plan'])
            if session.policy.agent.max_queue_seconds and hasattr(vector_tools,'scheduled_embed_query'):
                return {'embedding':await vector_tools.scheduled_embed_query(session,text)}
            return {'embedding': await session.tool(lambda: vector_tools.embed_query(text,
                budget_seconds=session.remaining_seconds))}

        async def search(state, session):
            from discovery_harness.coverage import groups_for_plan
            # Primary retrieval owns the full candidate budget. Related concepts
            # are searched only after this result is delivered, in a child job.
            related = False if judge else state['plan'].get('related_mode') != 'exclude'
            options = {'include_related': related} if getattr(vector_tools,'supports_exploration_control',False) else {}
            options['lexical_query']=str(state['plan'].get('semantic_query') or request_text(state))[:200]
            if getattr(vector_tools, 'supports_time_series_control', False):
                from discovery_harness.dataset_series import collapse_for_plan
                options['collapse_time_series'] = collapse_for_plan(state['plan'], request_text(state))
            filters=groups_for_plan(state['plan'])
            if filters:options['filter_groups']=filters
            if session.policy.agent.max_queue_seconds and hasattr(vector_tools,'scheduled_cosine_search'):
                result = await vector_tools.scheduled_cosine_search(session,state['embedding'],state['plan']['source_ids'],**options)
            else:
                result = await session.tool(lambda: vector_tools.cosine_search(
                state['embedding'], state['plan']['source_ids'],budget_seconds=session.remaining_seconds,
                **options, **({'queued':True} if session.policy.agent.timeout_seconds>20 else {})))
            if (not related or state['plan'].get('related_exclusions')) and 'exploration' in result:
                result = copy.deepcopy(result)
                result['exploration'].update(related_topics=[],relations=[])
                result['retrieval']['related_expansion_disabled'] = True
            from discovery_harness.similarity_results import select_candidates
            candidates = result['candidates']
            selected = select_candidates(state['plan'], candidates, self.sites.sources, limit=40 if judge else 10, filter_related=not bool(judge))
            result = {**result, 'candidates': selected, 'retrieval': {**result['retrieval'],
                'selection': {'candidate_datasets': len(candidates),
                              'candidate_sites': len({r['source_id'] for r in candidates if isinstance(r,dict) and isinstance(r.get('source_id'),str)}),
                              'selected_datasets': len(selected)}}}
            return {'search': result,'embedding':None}

        async def assess_relevance(state, session):
            direct_plan={**state['plan'],'related_mode':'exclude'}
            result = await judge(session, request_text(state), direct_plan, state['search']['candidates'])
            return {'search': {**state['search'], 'candidates': result['candidates'],
                'retrieval': {**state['search']['retrieval'], 'relevance': result['assessment']}}}

        async def topic_context(state, session):
            from discovery_harness.topic_context import safe_topic_context, unavailable
            candidates = state['search']['candidates']
            ids = [candidate['dataset_id'] for candidate in candidates]
            result = unavailable()
            if ids:
                if session.policy.agent.max_queue_seconds:
                    result = await topic_lookup.scheduled_lookup(session, ids)
                else:
                    result = await session.tool(lambda: topic_lookup.lookup(ids,
                        budget_seconds=session.remaining_seconds))
            contexts = result['contexts']
            annotated = []
            for candidate in candidates:
                item = {key: value for key, value in candidate.items() if key != 'topic_context'}
                context = safe_topic_context(contexts.get(candidate['dataset_id']))
                if context is not None:
                    item['topic_context'] = context
                annotated.append(item)
            retrieval = {**state['search']['retrieval'], 'topic_classification': {
                'status': result['status'], 'version': result['version'],
                'count': len(contexts), 'requested_count': len(ids),
                'missing_count': len(result['missing_ids'])}}
            return {'search': {**state['search'], 'candidates': annotated, 'retrieval': retrieval}}

        async def rank_results(state, session):
            from discovery_harness.similarity_results import compose_similarity_result
            try:
                result = compose_similarity_result(state['plan'], state['search']['candidates'],
                    self.sites.sources, state['search']['retrieval'])
                if 'exploration' in state['search']:
                    from discovery_harness.exploration_contract import validate_exploration
                    result['exploration'] = validate_exploration(state['search']['exploration'])
                return {'result': result, 'search':None}
            except (ValueError, TypeError, KeyError) as error:
                raise AdapterContractError('Invalid vector evidence result') from error

        async def validate(state, session):
            try:
                return {'result': validate_site_result(state['result'])}
            except ValueError as error:
                raise AdapterContractError('Invalid evidence-backed result') from error

        for name, operation in [('interpret', interpret_intent), ('normalize', normalize),
                                ('clarify', clarify), ('traverse', traverse),
                                ('evidence', evidence), ('embed_query', embed),
                                ('cosine_search', search), ('topic_context', topic_context),
                                ('assess_relevance', assess_relevance),
                                ('rank_results', rank_results),
                                ('validate', validate)]:
            node(name, operation)
        graph.add_conditional_edges(START, lambda s: 'interpret' if 'query' in s else 'normalize',
                                    ['interpret', 'normalize'])
        graph.add_edge('interpret', 'normalize')
        graph.add_conditional_edges('normalize', lambda s: 'clarify' if s['plan']['question'] else
                                    'embed_query' if vector_tools else 'traverse',
                                    ['clarify', 'traverse', 'embed_query'])
        graph.add_edge('embed_query', 'cosine_search')
        after_relevance = 'topic_context' if topic_lookup else 'rank_results'
        graph.add_edge('cosine_search', 'assess_relevance' if judge else after_relevance)
        graph.add_edge('assess_relevance', after_relevance)
        graph.add_edge('topic_context', 'rank_results')
        graph.add_edge('rank_results', 'validate')
        graph.add_edge('traverse', 'evidence')
        graph.add_edge('evidence', 'validate')
        graph.add_edge('clarify', 'validate')
        graph.add_edge('validate', END)
        self.graph = graph.compile()

    def memory_status(self):
        return {'active':self.metadata_active,'queued':self.metadata_waiters,'peak_active':self.metadata_peak,
                'limit':25,'max_search_response_bytes':524288,'max_retained_response_wire_bytes':25*524288}

    @asynccontextmanager
    async def metadata_slot(self,session):
        if session.policy.agent.max_queue_seconds:
            async with session.waiting_slot(self.metadata_slots):yield
        else:
            # Legacy synchronous calls keep their original wall-clock budget.
            async with asyncio.timeout(session.remaining_seconds):
                await self.metadata_slots.acquire()
            try:yield
            finally:self.metadata_slots.release()

    async def run(self, session, *, query=None, previous=None, plan=None):
        context = RequestContext(session)
        state = {'query': query, 'previous': previous} if query is not None else {'plan': plan}
        # Explicitly disable tracing even if the host has LangSmith env settings.
        try:
            with tracing_context(enabled=False):
                output = await self.graph.ainvoke(state, context=context,
                    config={'recursion_limit': self.recursion_limit, 'callbacks': []})
        except GraphRecursionError as error:
            raise BudgetExceeded('graph_steps', self.recursion_limit) from error
        finally:
            if context.metadata_lease is not None:
                try:await context.metadata_lease.__aexit__(None,None,None)
                finally:self.metadata_active-=1
        result = output['result']
        result['workflow'] = {'engine': 'langgraph', 'mode': 'question' if query is not None else 'node',
                              'recursion_limit': self.recursion_limit, 'stages': context.stages,
                              'retrieval_mode': ('embedding_relevance_then_cosine' if self.judge else 'embedding_cosine_ranked') if self.vector_tools else 'curated_evidence'}
        bounded_json(result, session.policy.agent.max_result_bytes)
        return result
