import datetime as dt
import json
import sqlite3
import unittest
from unittest.mock import patch
from tools.attachments import test_cloud_client as fixture
from tools.attachments import cloud_client as cc

class CloudRetention(unittest.TestCase):
    setUp=fixture.CloudTests.setUp
    tearDown=fixture.CloudTests.tearDown
    client=fixture.CloudTests.client
    def test_revoke_during_transport_discards_body_but_accounts_spend(self):
        authorized=[True]
        def transport(body,key):
            authorized[0]=False
            return fixture.response(text='revoked response private marker')
        client=self.client(transport=transport,authorize=lambda:authorized[0])
        with self.assertRaises(cc.AuthorizationDenied):client.analyze(fixture.CONTENT)
        usage=client.usage_summary()
        self.assertAlmostEqual(usage['cost_usd'],.0003)
        with client._db() as db:
            status,result=db.execute('SELECT status,result FROM requests').fetchone()
        self.assertEqual(status,'denied_spent');self.assertIsNone(result)
        with open(self.state,'rb') as stream:self.assertNotIn(b'revoked response private marker',stream.read())
    def test_revoke_with_unknown_usage_keeps_reservation(self):
        authorized=[True]
        def transport(body,key):authorized[0]=False;return b'bad response'
        client=self.client(transport=transport,authorize=lambda:authorized[0])
        with self.assertRaises(cc.AuthorizationDenied):client.analyze(fixture.CONTENT)
        self.assertGreater(client.usage_summary()['reserved_usd'],0)
    def test_cache_eviction_preserves_current_cost_and_retry_rows(self):
        with patch.object(cc,'CACHE_MAX_RESULTS',2,create=True):
            client=self.client()
            for n in range(5):client.analyze([{'type':'input_text','text':str(n)}])
            with client._db() as db:
                self.assertLessEqual(db.execute('SELECT count(*) FROM requests WHERE result IS NOT NULL').fetchone()[0],2)
                self.assertEqual(db.execute('SELECT count(*) FROM requests').fetchone()[0],5)
            self.assertAlmostEqual(client.usage_summary()['cost_usd'],.0015)
    def test_prior_month_pruned_and_current_reservations_preserved(self):
        client=self.client();client.analyze(fixture.CONTENT)
        with client._db() as db:
            db.execute("INSERT INTO requests(fingerprint,month,started,status,reserved,result) VALUES('old','2000-01',0,'failed',2,'private')")
            month=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m')
            db.execute("INSERT INTO requests(fingerprint,month,started,status,reserved) VALUES('current',?,0,'failed',3)",(month,))
        client=self.client()
        with client._db() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM requests WHERE month='2000-01'").fetchone()[0],0)
            self.assertGreaterEqual(len(db.execute('PRAGMA index_list(requests)').fetchall()),3)
        self.assertEqual(client.usage_summary()['reserved_usd'],3)
    def test_expired_cache_does_not_erase_retry_or_cost_accounting(self):
        client=self.client();client.analyze(fixture.CONTENT)
        with client._db() as db:db.execute('UPDATE requests SET started=0')
        client=self.client()
        with client._db() as db:self.assertIsNone(db.execute('SELECT result FROM requests').fetchone()[0])
        client.analyze(fixture.CONTENT)
        self.assertEqual(client.usage_summary()['requests'],2)
        self.assertAlmostEqual(client.usage_summary()['cost_usd'],.0006)
    def test_outbound_authorization_exception_keeps_ambiguous_reservation(self):
        def transport(body,key):raise cc.AuthorizationDenied('revoked after outbound')
        client=self.client(transport=transport)
        with self.assertRaises(cc.AuthorizationDenied):client.analyze(fixture.CONTENT)
        self.assertGreater(client.usage_summary()['reserved_usd'],0)
        with client._db() as db:self.assertEqual(db.execute('SELECT status FROM requests').fetchone()[0],'denied_spent')
    def test_request_cap_preserves_budget_and_avoids_new_transport(self):
        with patch.object(cc,'MAX_MONTH_REQUESTS',2,create=True):
            client=self.client()
            for n in range(2):client.analyze([{'type':'input_text','text':str(n)}])
            with self.assertRaises(cc.BudgetExceeded):client.analyze([{'type':'input_text','text':'third'}])
            self.assertEqual(len(self.calls),2)

if __name__=='__main__':unittest.main()
