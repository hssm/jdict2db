# -*- coding: utf-8 -*-
#Copyright (C) 2011 Houssam Salem <ntsp.gm@gmail.com>
#License: GPLv3; http://www.gnu.org/licenses/gpl.txt

import os
import unittest
from sqlalchemy.sql import select
from sqlalchemy import create_engine
from jdict2db.jmdict import * 
 

#Set this to True to create a database which will be reused in future tests
#while kept True. You'll have to delete it manually from the test_dbs folder
#if you need to rebuild it.
reuse_db = False

if reuse_db:
    if not os.path.exists('test_dbs/test_jmdict.sqlite'):
        fill_database('../data/JMdict',
                         'sqlite:///test_dbs/test_jmdict.sqlite')
        from jdict2db.jmdict import conn
    else:
        conn = create_engine('sqlite:///test_dbs/test_jmdict.sqlite',
                              echo=False)
else:
    fill_database('../data/JMdict', 'sqlite:///') #in-memory db
    from jdict2db.jmdict import conn

class TestKanjidicEntries(unittest.TestCase):
    
    def m_check_matches(self, table, fk_name, fk_value, expect_n_rows,
                         expect_list):
        s = select([table], table.c[fk_name] == fk_value)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == expect_n_rows,
                         "Expected %d rows but found %s " % (expect_n_rows,
                                                              len(rows)))

        for (expect_row, result_row) in zip(expect_list, rows):
            for key in expect_row.keys():
                self.assertTrue(expect_row[key] == result_row[key],
                                 "%s error. Expected %s but found %s." %
                                  (key, expect_row[key], result_row[key]))

    def test_k_ele(self):
        """
        Test k_ele and sub-elements (i.e., tables that use it as a foreign key)
        """
        ent_seq = 1170650
        s = select([k_ele], k_ele.c.entry_ent_seq==ent_seq)
        result = conn.execute(s)
        rows = result.fetchall()        
        expect = [{'keb':'隠す'},
                  {'keb':'隠くす'},
                  {'keb':'匿す'},
                  {'keb':'隱くす'}]
        self.m_check_matches(k_ele, 'entry_ent_seq', ent_seq, 4, expect)
        
        inf_n = [0, 1, 0, 2]
        inf_expect = [
                      [],
                      [{'ke_inf':'irregular okurigana usage'}],
                      [{}],
                      [{'ke_inf':'irregular okurigana usage'},
                       {'ke_inf':'word containing out-dated kanji'}]
                      ]
        
        pri_n = [3, 0, 0, 0]
        pri_expect = [
                      [{'ke_pri':'ichi1'},
                       {'ke_pri':'news1'},
                       {'ke_pri':'nf12'}],
                      [{}],
                      [{}],
                      [{}]
                      ]
        
        for (i, row) in enumerate(rows):
            self.m_check_matches(ke_inf, 'k_ele_id', row.id, inf_n[i],
                                  inf_expect[i])
            self.m_check_matches(ke_pri, 'k_ele_id', row.id, pri_n[i],
                                  pri_expect[i])

    def test_r_ele(self):
        """
        Test r_ele and sub-elements (i.e., tables that use it as a foreign key)
        """
        ent_seq = 1369900 

        s = select([r_ele], r_ele.c.entry_ent_seq==ent_seq)
        result = conn.execute(s)
        rows = result.fetchall()        
        
        expect = [{'reb':'ごみ', 're_nokanji':False},
                  {'reb':'ゴミ', 're_nokanji':True},
                  {'reb':'あくた', 're_nokanji':False}]
        self.m_check_matches(r_ele, 'entry_ent_seq', ent_seq, 3, expect)
        
        pri_n = [3, 1, 0]
        pri_expect = [
                      [{'re_pri':'ichi1'},
                       {'re_pri':'news2'},
                       {'re_pri':'nf36'}],
                      [{'re_pri':'gai1'}],
                      [{}]
                      ]
        
        restr_n = [0, 0, 1]
        restr_expect = [
                      [{}],
                      [{}],
                      [{'re_restr':'芥'}]
                      ]
        
        for (i, row) in enumerate(rows):
            self.m_check_matches(re_pri, 'r_ele_id', row.id, pri_n[i],
                                 pri_expect[i])
            self.m_check_matches(re_restr, 'r_ele_id', row.id, restr_n[i],
                                 restr_expect[i])
            
        #Let's do another one. The one above didn't have any re_inf
        ent_seq = 1061830 

        s = select([r_ele], r_ele.c.entry_ent_seq==ent_seq)
        result = conn.execute(s)
        rows = result.fetchall()        
        
        expect = [{'reb':'シャン', 're_nokanji':False},
                  {'reb':'シヤン', 're_nokanji':False},]
        self.m_check_matches(r_ele, 'entry_ent_seq', ent_seq, 2, expect)
        
        pri_n = [1, 0]
        pri_expect = [
                      [{'re_pri':'gai1'},],
                      [{}]
                     ]
        
        inf_n = [0, 1]
        inf_expect = [
                      [{}],
                      [{'re_inf':'word containing irregular kana usage'}],
                      ]
        
        for (i, row) in enumerate(rows):
            self.m_check_matches(re_pri, 'r_ele_id', row.id, pri_n[i],
                                 pri_expect[i])
            self.m_check_matches(re_inf, 'r_ele_id', row.id, inf_n[i],
                                 inf_expect[i])

    def test_info(self):
        """
        Test info and sub-elements (i.e., tables that use it as a foreign key)
        """
        ent_seq = 1050400
        s = select([info], info.c.entry_ent_seq==ent_seq)
        result = conn.execute(s)
        rows = result.fetchall()        
        
        self.assertTrue(len(rows) == 1, "Expected 1 info rows but found %s " %
                        len(rows))
        
        #It's a 0 or 1 field, and we know one exists
        info_id = rows[0]['id']
        
        #TODO: need test data for these; there are none in the actual file
        expect = []
        self.m_check_matches(links, 'info_id', info_id, 0, expect)
        expect = []
        self.m_check_matches(bibl, 'info_id', info_id, 0, expect)
        expect = []
        self.m_check_matches(etym, 'info_id', info_id, 0, expect)
        
        
        expect = [{'upd_date':'2010-06-02', 'upd_detl':'Entry created'}]
        self.m_check_matches(audit, 'info_id', info_id, 1, expect)        

    def test_sense(self):
        ent_seq = 1010230
        s = select([sense], sense.c.entry_ent_seq==ent_seq)
        result = conn.execute(s)
        rows = result.fetchall()        
        
        self.assertTrue(len(rows) == 2, "Expected 2 sense rows but found %s " %
                        len(rows))

    def test_stagk(self):
        s = select([sense], sense.c.entry_ent_seq==1150140)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 2, "Expected 2 sense rows but found %s " %
                        len(rows))
        sense_id = rows[1].id   #no stagk in 0th sense
        expect = [{'stagk':'哀れむ'}]
        self.m_check_matches(stagk, 'sense_id', sense_id, 1, expect)
    
    def test_stagr(self):
        s = select([sense], sense.c.entry_ent_seq==1152850)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 3, "Expected 3 sense rows but found %s " %
                        len(rows))
        
        stagr_n = [2, 2, 0]
        stagr_expect = [
                        [{'stagr':'あずさ'},{'stagr':'アズサ'}],
                        [{'stagr':'あずさ'},{'stagr':'アズサ'}],
                        []
                       ]
        for (i, row) in enumerate(rows):
            self.m_check_matches(stagr, 'sense_id', row.id, stagr_n[i],
                                 stagr_expect[i])
    
    def test_pos(self):
        s = select([sense], sense.c.entry_ent_seq==1152920)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 1, "Expected 1 sense row but found %s " %
                        len(rows))
        sense_id = rows[0].id
        expect = [{'pos':"Godan verb with `su' ending"},
                  {'pos':'transitive verb'}]
        self.m_check_matches(pos, 'sense_id', sense_id, 2, expect)
        
    def test_xref(self):
        s = select([sense], sense.c.entry_ent_seq==1154750)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 1, "Expected 1 sense row but found %s " %
                        len(rows))
        sense_id = rows[0].id
        expect = [{'xref':'隠喩'}]
        self.m_check_matches(xref, 'sense_id', sense_id, 1, expect)
    
    def test_ant(self):        
        s = select([sense], sense.c.entry_ent_seq==1170230)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 2, "Expected 2 sense rows but found %s " %
                        len(rows))
        sense_id = rows[0].id
        expect = [{'ant':'陽'}]
        self.m_check_matches(ant, 'sense_id', sense_id, 1, expect)
    
    def test_field(self):
        s = select([sense], sense.c.entry_ent_seq==1183690)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 1, "Expected 1 sense row but found %s " %
                        len(rows))
        sense_id = rows[0].id
        expect = [{'field':'linguistics terminology'}]
        self.m_check_matches(field, 'sense_id', sense_id, 1, expect)
        
    def test_misc(self):
        s = select([sense], sense.c.entry_ent_seq==1001180)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 3, "Expected 3 sense rows but found %s " %
                        len(rows))
        sense_id = rows[0].id   #no misc in other two
        expect = [{'misc':'word usually written using kana alone'},
                  {'misc':'honorific or respectful (sonkeigo) language'}
                 ]
        self.m_check_matches(misc, 'sense_id', sense_id, 2, expect)
        
    def test_s_inf(self):
        s = select([sense], sense.c.entry_ent_seq==1001990)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 4, "Expected 4 sense rows but found %s " %
                        len(rows))
        n = [1, 0, 1, 1]
        expect = [
                        [{'s_inf':'usu. お姉さん'}],
                        [],
                        [{'s_inf':'usu. お姐さん'}],
                        [{'s_inf':'usu. お姐さん'}],
                       ]
        for (i, row) in enumerate(rows):
            self.m_check_matches(s_inf, 'sense_id', row.id, n[i], expect[i])
    
    def test_lsource(self):
        s = select([sense], sense.c.entry_ent_seq==1041440)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 1, "Expected 1 sense row but found %s " %
                        len(rows))
        sense_id = rows[0].id
        expect = [{'lsource':'cash-service corner', 'lang':'eng',
                   'ls_type':'full', 'ls_wasei':True}]
        self.m_check_matches(lsource, 'sense_id', sense_id, 1, expect)        
    
        #we'll do one with a different language and ls_wasei=False
        s = select([sense], sense.c.entry_ent_seq==1043240)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 1, "Expected 1 sense row but found %s " %
                        len(rows))
        sense_id = rows[0].id
        expect = [{'lsource':None, 'lang':'fre', 'ls_type':'full',
                   'ls_wasei':False}]
        self.m_check_matches(lsource, 'sense_id', sense_id, 1, expect)
        
    def test_dial(self):
        s = select([sense], sense.c.entry_ent_seq==1188610)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 1, "Expected 1 sense row but found %s " %
                        len(rows))
        sense_id = rows[0].id
        expect = [{'dial':'Kansai-ben'}]
        self.m_check_matches(dial, 'sense_id', sense_id, 1, expect)
            
    def test_gloss(self):
        s = select([sense], sense.c.entry_ent_seq==1188890)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 2, "Expected 2 sense rows but found %s " %
                        len(rows))
        n = [7, 2]
        expect = [
                 [{'gloss':'always', 'lang':'eng', 'g_gend':None},
                  {'gloss':'usually', 'lang':'eng', 'g_gend':None},
                  {'gloss':'every time', 'lang':'eng', 'g_gend':None},
                  {'gloss':'à chaque fois', 'lang':'fre', 'g_gend':None},
                  {'gloss':'(nég) jamais', 'lang':'fre', 'g_gend':None},
                  {'gloss':'habituellement', 'lang':'fre', 'g_gend':None},
                  {'gloss':'toujours', 'lang':'fre', 'g_gend':None}],
                 [{'gloss':'never (with neg. verb)', 'lang':'eng', 'g_gend':None},
                  {'gloss':'(n) immer', 'lang':'ger', 'g_gend':None}],
                 ]
        for (i, row) in enumerate(rows):
            self.m_check_matches(gloss, 'sense_id', row.id, n[i], expect[i])
        
        #TODO: Need tests for g_gend. There are none in the dictionary
    
    def test_example(self):
        #TODO: There are no examples in the dictionary to test.
        pass