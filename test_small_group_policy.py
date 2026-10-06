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
