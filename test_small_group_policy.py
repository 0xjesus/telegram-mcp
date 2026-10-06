import sqlite3,unittest
import consent

class PolicyTests(unittest.TestCase):
 def setUp(self):
  self.db=sqlite3.connect(':memory:')
  self.addCleanup(self.db.close)
  self.db.execute('CREATE TABLE chats(id INTEGER PRIMARY KEY,type TEXT,backfill_done INTEGER DEFAULT 0)')
  self.db.executemany('INSERT INTO chats(id,type) VALUES(?,?)',[(-1,'group'),(-2,'supergroup'),(-3,'group')])
  consent.install(self.db)
  consent.configure_policy(self.db,True,10,'synthetic policy authorization')
 def test_threshold_unknown_and_manual_off(self):
  consent.refresh_group(self.db,-1,'Small',10)
  consent.refresh_group(self.db,-2,'Large',11)
  consent.refresh_group(self.db,-3,'Unknown',None)
  self.assertTrue(consent.allowed(self.db,-1))
  self.assertFalse(consent.allowed(self.db,-2))
  self.assertFalse(consent.allowed(self.db,-3))
  consent.set_consent(self.db,-1,False,'manual off')
  consent.refresh_group(self.db,-1,'Small',3)
  self.assertFalse(consent.allowed(self.db,-1))
 def test_expiry_growth_and_manual_allow(self):
  consent.refresh_group(self.db,-1,'Small',2)
  self.db.execute("UPDATE group_monitoring_consent SET updated_at='2000-01-01T00:00:00Z'")
  self.assertFalse(consent.allowed(self.db,-1))
  consent.refresh_group(self.db,-1,'Small',10)
  self.assertTrue(consent.allowed(self.db,-1))
  consent.refresh_group(self.db,-1,'Growing',11)
  self.assertFalse(consent.allowed(self.db,-1))
  consent.set_consent(self.db,-1,True,'manual allow',True)
  consent.refresh_group(self.db,-1,'Large',100)
  self.assertTrue(consent.allowed(self.db,-1))
 def test_disable_and_missing_groups(self):
  consent.refresh_group(self.db,-1,'Small',3)
  consent.finish_inventory(self.db,[])
  self.assertFalse(consent.allowed(self.db,-1))
  consent.refresh_group(self.db,-1,'Small',3)
  consent.configure_policy(self.db,False,10,'disabled')
  self.assertFalse(consent.allowed(self.db,-1))
 def test_metadata_cadence_and_policy_off(self):
  consent.refresh_group(self.db,-2,'Small',3,verified_at='2000-01-01T00:00:00Z',attempted_at=1000)
  self.assertFalse(consent.metadata_plan(self.db,-2,1299)['fetch'])
  self.assertTrue(consent.metadata_plan(self.db,-2,1300)['fetch'])
  consent.refresh_group(self.db,-2,'Large',11,attempted_at=1000)
  self.assertFalse(consent.metadata_plan(self.db,-2,1300)['fetch'])
  self.assertTrue(consent.metadata_plan(self.db,-2,4600)['fetch'])
  consent.refresh_group(self.db,-2,'Unknown',None,attempted_at=5000)
  self.assertFalse(consent.metadata_plan(self.db,-2,5300)['fetch'])
  consent.configure_policy(self.db,False,10,'off')
  self.assertFalse(consent.metadata_plan(self.db,-2,10000)['fetch'])
 def test_cached_timestamp_expires_and_membership_invalidates(self):
  old='2000-01-01T00:00:00Z'
  consent.refresh_group(self.db,-2,'Small',3,verified_at=old,attempted_at=1000)
  plan=consent.metadata_plan(self.db,-2,1100)
  consent.refresh_group(self.db,-2,'Cached',plan['count'],verified_at=plan['verified_at'])
  self.assertFalse(consent.allowed(self.db,-2))
  self.assertEqual(self.db.execute('SELECT updated_at FROM group_monitoring_inventory WHERE chat_id=-2').fetchone()[0],old)
  consent.invalidate_group(self.db,-2)
  plan=consent.metadata_plan(self.db,-2,1100)
  self.assertTrue(plan['fetch']);self.assertIsNone(plan['count'])
 def test_manual_override_skips_rpc(self):
  consent.set_consent(self.db,-2,False,'manual off')
  self.assertFalse(consent.metadata_plan(self.db,-2,10000)['fetch'])
