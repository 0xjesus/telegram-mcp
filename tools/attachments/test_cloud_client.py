import concurrent.futures
import os
import tempfile
import threading
import unittest
from unittest.mock import patch
from tools.attachments import cloud_client as cc

CONTENT = [{'type': 'input_text', 'text': 'synthetic note'}]
def response(status='completed', text='Descripción', usage=None):
    return {'status': status, 'output': [{'content': [{'type': 'output_text', 'text': text}]}], 'usage': usage or {'input_tokens': 100, 'output_tokens': 50}}

class CloudTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = os.path.join(self.tmp.name, 'private', 'state.sqlite')
        self.key = os.path.join(self.tmp.name, 'synthetic.key')
        with open(self.key, 'w') as f: f.write('synthetic-key')
        self.calls = []
    def tearDown(self): self.tmp.cleanup()
    def client(self, **kw):
        def transport(body, key):
            self.calls.append(body)
            self.assertEqual(key, 'synthetic-key')
            return response()
        return cc.CloudClient(self.state, self.key, transport=kw.pop('transport', transport), **kw)
    def test_durable_cache_usage_and_private_permissions(self):
        result = self.client().analyze(CONTENT)
        self.assertTrue(result['complete'])
        self.assertFalse(result['cached'])
        self.assertAlmostEqual(result['cost_usd'], .0003)
        self.assertTrue(self.client().analyze(CONTENT)['cached'])
        self.assertEqual(len(self.calls), 1)
        usage = self.client().usage_summary()
        self.assertEqual(usage['requests'], 1)
        self.assertEqual(usage['reserved_usd'], 0)
        self.assertEqual(os.stat(self.state).st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(os.path.dirname(self.state)).st_mode & 0o777, 0o700)
        self.assertFalse(self.calls[0]['store'])
        self.assertEqual(self.calls[0]['reasoning'], {'effort': 'none'})
    def test_budget_prevents_http(self):
        with self.assertRaises(cc.BudgetExceeded): self.client(monthly_budget=.000001).analyze(CONTENT)
        self.assertFalse(self.calls)
    def test_ambiguous_failure_retains_reservation_and_limits_attempts(self):
        def fail(body, key): raise OSError('secret body synthetic-key')
        client = self.client(transport=fail)
        for _ in range(3):
            with self.assertRaises(cc.CloudError) as caught: client.analyze(CONTENT)
            self.assertNotIn('secret', str(caught.exception))
        self.assertGreater(client.usage_summary()['reserved_usd'], 0)
        with self.assertRaises(cc.RetryExhausted): client.analyze(CONTENT)
        with client._db() as db:
            db.execute("UPDATE requests SET month='2000-01'")
        with self.assertRaises(cc.CloudError): client.analyze(CONTENT)
        self.assertEqual(client.usage_summary()['requests'],1)
    def test_partial_cached_honestly_and_refusal_errors(self):
        client = self.client(transport=lambda b,k: response('incomplete', 'partial'))
        self.assertFalse(client.analyze(CONTENT)['complete'])
        self.assertTrue(client.analyze(CONTENT)['cached'])
        with self.assertRaises(cc.CloudError): self.client(transport=lambda b,k: response(text='')).analyze([{'type':'input_text','text':'other'}])
    def test_authorization_cache_and_before_send(self):
        client = self.client()
        client.analyze(CONTENT)
        with self.assertRaises(cc.AuthorizationDenied): self.client(authorize=lambda: False).analyze(CONTENT)
        checks = iter([True, False])
        with self.assertRaises(cc.AuthorizationDenied): self.client(authorize=lambda: next(checks)).analyze([{'type':'input_text','text':'revoked'}])
        self.assertEqual(len(self.calls), 1)
    def test_concurrent_fingerprint_sends_once(self):
        entered, release = threading.Event(), threading.Event()
        def slow(body,key):
            self.calls.append(body); entered.set(); release.wait(3); return response()
        client = self.client(transport=slow)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            first = pool.submit(client.analyze, CONTENT)
            self.assertTrue(entered.wait(2))
            with self.assertRaises(cc.RequestDeferred): client.analyze(CONTENT)
            release.set(); first.result()
        self.assertEqual(len(self.calls), 1)
    def test_revocation_after_reservation_releases_unspent_cost(self):
        checks = iter([True, True, False])
        client = self.client(authorize=lambda: next(checks))
        with self.assertRaises(cc.AuthorizationDenied): client.analyze(CONTENT)
        self.assertFalse(self.calls)
        self.assertEqual(client.usage_summary()['reserved_usd'], 0)
    def test_response_size_is_bounded_and_private_input_is_not_persisted(self):
        client = self.client(transport=lambda b,k: b'x' * (cc.MAX_BODY + 1))
        with self.assertRaises(cc.CloudError): client.analyze(CONTENT)
        with open(self.state, 'rb') as f:
            self.assertNotIn(b'synthetic note', f.read())
    def test_invalid_budget_and_model(self):
        for budget in [0, -1, float('nan'), float('inf')]:
            with self.assertRaises(ValueError): self.client(monthly_budget=budget)
        with self.assertRaises(ValueError): self.client(model='anything')
    def test_production_transport_blocks_redirect_and_sanitizes_body(self):
        client = cc.CloudClient(self.state, self.key)
        import urllib.error
        with patch('urllib.request.OpenerDirector.open', side_effect=urllib.error.HTTPError('https://api.openai.com/v1/responses',302,'private body',{},None)):
            with self.assertRaises(cc.CloudError) as caught: client.analyze(CONTENT)
        self.assertNotIn('private', str(caught.exception))
        self.assertGreater(client.usage_summary()['reserved_usd'], 0)

if __name__ == '__main__': unittest.main()
