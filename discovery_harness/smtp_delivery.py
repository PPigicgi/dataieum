"""One authenticated service SMTP sender; verified users appear in Reply-To."""
import asyncio
from datetime import datetime, timezone
from email.message import EmailMessage
from email.policy import SMTP as SMTP_POLICY
from email.utils import format_datetime, parseaddr
import re
import smtplib
import ssl
import time
import uuid

from .cooperation import CooperationError, email, sender, text
from .mail_delivery import DeliveryError


class _Deadline:
    def __init__(self, *args, deadline, **kwargs):
        self.deadline = deadline
        super().__init__(*args, **kwargs)

    def getreply(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:raise TimeoutError('SMTP deadline exceeded')
        if self.sock is not None:self.sock.settimeout(min(10, remaining))
        return super().getreply()


class _SMTP(_Deadline, smtplib.SMTP):pass
class _SMTPSSL(_Deadline, smtplib.SMTP_SSL):pass


class SMTPDelivery:
    def __init__(self, config, password, from_address):
        if (not isinstance(config, dict) or set(config) != {'host','port','security','username'}
                or not isinstance(config['host'], str) or len(config['host']) > 253
                or not re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?', config['host'])
                or type(config['port']) is not int or not 1 <= config['port'] <= 65535
                or config['security'] not in {'ssl','starttls'}):
            raise ValueError('Invalid SMTP configuration')
        self.host, self.port, self.security = config['host'], config['port'], config['security']
        self.username = text(config['username'], 320, line=True)
        self.password = text(password, 4096, line=True)
        self.from_address = sender(from_address)
        self.envelope_from = email(parseaddr(self.from_address)[1])
        self.context = ssl.create_default_context()
        self._inflight = None

    def _message(self, message):
        payload = message['payload']
        if payload.get('from') != self.from_address or not isinstance(payload.get('to'), list) or len(payload['to']) != 1:
            raise ValueError('Invalid fixed sender or recipient')
        recipient = email(payload['to'][0])
        identifier = str(uuid.UUID(message['message_id']))
        mime = EmailMessage(policy=SMTP_POLICY)
        mime['From'], mime['To'] = self.from_address, recipient
        if payload.get('reply_to'):mime['Reply-To'] = email(payload['reply_to'])
        mime['Subject'] = text(payload['subject'], 1000, line=True)
        mime['Date'] = format_datetime(datetime.fromtimestamp(message['created_at'], timezone.utc))
        mime['Message-ID'] = '<'+identifier+'@'+self.envelope_from.split('@')[1]+'>'
        mime.set_content(text(payload['text'], 50000), charset='utf-8', cte='quoted-printable')
        return mime, recipient, mime.as_bytes()

    def _send(self, message):
        try:mime, recipient, wire = self._message(message)
        except (KeyError, TypeError, ValueError, CooperationError):
            raise DeliveryError('smtp_message_invalid') from None
        client, phase = None, 'connect'
        deadline = time.monotonic() + 20
        def check():
            remaining = deadline-time.monotonic()
            if remaining <= 0:raise TimeoutError('SMTP deadline exceeded')
            if client.sock is not None:client.sock.settimeout(min(10,remaining))
        try:
            factory = _SMTPSSL if self.security == 'ssl' else _SMTP
            client = factory(self.host, self.port, timeout=10, deadline=deadline,
                             **({'context':self.context} if self.security == 'ssl' else {}))
            check();client.ehlo_or_helo_if_needed()
            if self.security == 'starttls':
                check();client.starttls(context=self.context)
                check();client.ehlo()
            check();client.login(self.username, self.password)
            check();code, _ = client.mail(self.envelope_from)
            if code != 250:raise smtplib.SMTPSenderRefused(code,b'',self.envelope_from)
            check();code, _ = client.rcpt(recipient)
            if code not in (250,251):raise smtplib.SMTPResponseException(code,b'')
            check();phase='data'
            code, _ = client.data(wire)
            if code != 250:raise smtplib.SMTPDataError(code,b'')
            return mime['Message-ID']
        except ssl.SSLError:
            raise DeliveryError('smtp_tls_failed', ambiguous=phase=='data') from None
        except smtplib.SMTPAuthenticationError as error:
            raise DeliveryError('smtp_auth_failed', retryable=400<=error.smtp_code<500) from None
        except smtplib.SMTPResponseException as error:
            # A definite negative SMTP response means this attempt was rejected.
            raise DeliveryError('smtp_rejected', retryable=400<=error.smtp_code<500) from None
        except (OSError, smtplib.SMTPServerDisconnected):
            raise DeliveryError('smtp_connection', retryable=phase!='data', ambiguous=phase=='data') from None
        except smtplib.SMTPException:
            raise DeliveryError('smtp_protocol', ambiguous=phase=='data') from None
        finally:
            if client is not None:
                # DATA 250 is acceptance; QUIT or close cannot reverse it.
                try:client.close()
                except (OSError,smtplib.SMTPException):pass

    async def deliver(self, message):
        # Do not spawn further work while a timed-out/cancelled thread finishes.
        if self._inflight is not None and not self._inflight.done():
            raise DeliveryError('smtp_busy', retryable=True)
        task = self._inflight = asyncio.create_task(asyncio.to_thread(self._send, message))
        task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        try:return await asyncio.wait_for(asyncio.shield(task),25)
        except TimeoutError:
            # Cancellation of to_thread is not proof that SMTP did not accept it.
            raise DeliveryError('smtp_timeout', ambiguous=True) from None

    async def aclose(self):
        # The worker's durable lease becomes unknown if shutdown interrupts it.
        # Never label cancellation as safe to resend.
        pass
