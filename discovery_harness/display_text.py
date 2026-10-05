"""Decode source HTML character references as text, never as trusted markup."""
import html
from html.parser import HTMLParser
import re


class _PlainText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True);self.parts=[];self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in {'script','style'}:self.hidden+=1
        elif tag in {'p','div','br','li','tr','h1','h2','h3'}:self.parts.append(' ')
    def handle_endtag(self,tag):
        if tag in {'script','style'}:self.hidden=max(0,self.hidden-1)
        elif tag in {'p','div','li','tr'}:self.parts.append(' ')
    def handle_data(self,data):
        if not self.hidden:self.parts.append(data)


def display_text(value):
    if not isinstance(value, str):
        return value
    text=html.unescape(value).replace('\xa0', ' ')
    if re.search(r'</?(?:p|div|br|a|span|ul|ol|li|table|tr|td|strong|em|b|i|h[1-6]|script|style)(?:\s|/?>)',text,re.I):
        parser=_PlainText();parser.feed(text);text=''.join(parser.parts)
    return re.sub(r'\s+',' ',text).strip()


_FIELDS = {'title', 'label', 'description', 'summary', 'name', 'publisher', 'region'}


def clean_response(value):
    if isinstance(value, list):
        return [clean_response(item) for item in value]
    if isinstance(value, dict):
        return {key: display_text(item) if key in _FIELDS and isinstance(item, str)
                else clean_response(item) for key, item in value.items()}
    return value
