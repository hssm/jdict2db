# -*- coding: utf-8 -*-
#Copyright (C) 2011 Houssam Salem <ntsp.gm@gmail.com>
#License: GPLv3; http://www.gnu.org/licenses/gpl.txt

import os
import unittest
from sqlalchemy.sql import select
from sqlalchemy import create_engine
from jdict2db.kanjidic import *


#Set this to True to create a database which will be reused in future tests
#while kept True You'll have to delete it manually from the test_dbs folder
#if you need to rebuild it.
reuse_db = False

if reuse_db:
    if not os.path.exists('test_dbs/kanjidic.sqlite'):
        fill_database('../data/kanjidic2.xml',
                         'sqlite:///test_dbs/kanjidic.sqlite')
        from jdict2db.kanjidic import conn
    else:
        conn = create_engine('sqlite:///test_dbs/kanjidic.sqlite',
                              echo=False)
else:
    fill_database('../data/kanjidic2.xml', 'sqlite:///') #in-memory db
    from jdict2db.kanjidic import conn


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
                
    def test_character(self):
        literal = u'今'
        expect= [{'literal':'今', 'grade':2, 'freq':49, 'jlpt':4}]
        self.m_check_matches(character, 'literal', literal, 1,
                             expect)
       
    def test_stroke_count(self):
        literal = u'収'
        s = select([stroke_count],stroke_count.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()        
        self.assertTrue(len(rows) == 2,
                        "Expected 2 stroke_count rows but found %s " %
                        len(rows))
        expect = [{'stroke_count':4},
                   {'stroke_count':5}]
        self.m_check_matches(stroke_count, 'character_literal', literal, 2,
                             expect)

    def test_variant(self):
        literal = u'穐'
        s = select([variant], variant.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 3,
                        "Expected 3 variant rows but found %s " % len(rows))
        expect = [{'var_type':'jis208', 'variant':'29-9'},
                   {'var_type':'jis208', 'variant':'67-52'},
                   {'var_type':'nelson_c', 'variant':'3273'}]
        self.m_check_matches(variant, 'character_literal', literal, 3,
                             expect)
        
    def test_rad_name(self):
        literal = u'禾'
        s = select([rad_name], rad_name.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()       
        self.assertTrue(len(rows) == 2,
                        "Expected 2 rad_name rows but found %s " % len(rows))
        expect = ['のぎ', 'のぎへん']
        for i, row in enumerate(rows):
            self.assertTrue(expect[i] == row['rad_name'],
                             "rad_name error. Expected %s but found %s " %
                             (expect[i], row['rad_name']))
        result.close()
        
    def test_dic_ref(self):
        literal = u'苛'
        s = select([dic_ref], dic_ref.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 4, 'Expected 4 dic_ref rows but found '\
                                        '%s ' % len(rows))
        
        expect = [{'dr_type':'nelson_c', 'dic_ref':'3925', 'm_vol':None,
                   'm_page':None},
                   {'dr_type':'nelson_n', 'dic_ref':'5006', 'm_vol':None,
                    'm_page':None},
                   {'dr_type':'heisig', 'dic_ref':'2373', 'm_vol':None,
                    'm_page':None},
                   {'dr_type':'moro', 'dic_ref':'30785X', 'm_vol':'9',
                    'm_page':'0568'}]
        self.m_check_matches(dic_ref, 'character_literal', literal, 4,
                             expect)
    
    def test_query_code(self):
        literal = u'苛'
        s = select([query_code], query_code.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 4, 'Expected 4 query_code rows but found '\
                                        '%s ' % len(rows))
        
        expect = [{'qc_type':'skip', 'q_code':'2-3-5', 'skip_misclass':None},
                  {'qc_type':'sh_desc', 'q_code':'3k5.30',
                   'skip_misclass':None},
                  {'qc_type':'four_corner', 'q_code':'4462.1',
                   'skip_misclass':None},
                  {'qc_type':'skip', 'q_code':'2-4-5',
                   'skip_misclass':'stroke_diff'}]
        self.m_check_matches(query_code, 'character_literal', literal, 4,
                             expect)            

    def test_codepoint(self):
        literal = u'嘩'
        s = select([codepoint], codepoint.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 2, 'Expected 2 codepoint rows but found '\
                                        '%s ' % len(rows))
        expect = [{'cp_value':'5629', 'cp_type':'ucs'},
                  {'cp_value':'18-62', 'cp_type':'jis208'}]
        self.m_check_matches(codepoint, 'character_literal', literal, 2,
                             expect)            
        
    def test_rad_value(self):
        literal = u'愛'
        s = select([rad_value], rad_value.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 2,
                        "Expected 2 rad_value rows but found %s " % len(rows))
        expect = [{'rad_value':61, 'rad_type':'classical'},
                  {'rad_value':87, 'rad_type':'nelson_c'}]
        self.m_check_matches(rad_value, 'character_literal', literal, 2,
                             expect)
    
    def test_reading(self):
        #NOTE: as of 2011/05/12, there are no cases with on_type or r_status
        #TODO: insert such cases in a test file in order to test the parsing 
        #behaviour
        
        literal = u'綻'
        s = select([reading], reading.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 5,
                        "Expected 2 reading rows but found %s " % len(rows))
        expect = [{'reading':'zhan4', 'r_type':'pinyin', 'on_type':None,
                   'r_status':None},
                  {'reading':'tan', 'r_type':'korean_r', 'on_type':None,
                   'r_status':None},
                  {'reading':'탄', 'r_type':'korean_h', 'on_type':None,
                   'r_status':None},
                  {'reading':'タン', 'r_type':'ja_on', 'on_type':None,
                   'r_status':None},
                  {'reading':'ほころ.びる', 'r_type':'ja_kun', 'on_type':None,
                   'r_status':None}]
            
        self.m_check_matches(reading, 'character_literal', literal, 5,
                             expect)

    def test_meaning(self):
        literal = u'歩'
        s = select([meaning], meaning.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 10,
                        "Expected 2 rad_value rows but found %s " % len(rows))
        expect = [{'meaning':'walk', 'm_lang':'en'},
                  {'meaning':'counter for steps', 'm_lang':'en'},
                  {'meaning':'marcher', 'm_lang':'fr'},
                  {'meaning':'compteur de pas', 'm_lang':'fr'},
                  {'meaning':'camino', 'm_lang':'es'},
                  {'meaning':'caminar', 'm_lang':'es'},
                  {'meaning':'razón', 'm_lang':'es'},
                  {'meaning':'proporción', 'm_lang':'es'},
                  {'meaning':'ir a pie', 'm_lang':'es'},
                  {'meaning':'passeio', 'm_lang':'pt'}]
            
        self.m_check_matches(meaning, 'character_literal', literal, 10,
                             expect)

    def test_nanori(self):
        literal = u'胆'
        s = select([nanori], nanori.c.character_literal==literal)
        result = conn.execute(s)
        rows = result.fetchall()
        self.assertTrue(len(rows) == 2,
                        "Expected 2 nanori rows but found %s " % len(rows))
        expect = [{'nanori':'い'},
                  {'nanori':'まこと'}]
            
        self.m_check_matches(nanori, 'character_literal', literal, 2,
                             expect)
