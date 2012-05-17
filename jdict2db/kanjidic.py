# -*- coding: utf-8 -*-
#Copyright (C) 2011 Houssam Salem <ntsp.gm@gmail.com>
#License: GPLv3; http://www.gnu.org/licenses/gpl.txt

import os
import time
import sys
from xml.etree.cElementTree import iterparse
from sqlalchemy import create_engine, Table, Column, Integer, String, Unicode,\
                       ForeignKey, MetaData
import download


KANJIDIC2_PATH = '../data/kanjidic2.xml'

metadata = MetaData()
engine = None
conn = None

#All table_l variables are both global and are referenced inside all_l.
#table_l values are lists of pending row insertions. They are global as
#they need to be conveniently accessed to insert data into them. all_l will be
#a list of lists. Each sub-list = [table_l, table.insert()]
#table.insert() is a re-usable insert statement for that table (it's identical
#each time, so no need to keep requesting it)
all_l = []

#value to use as number of rows needed in a table_l list before we save it
#(large performance improvement by having fewer commits with more data)
n_to_commit = 10000

#Set up database tables
character = Table('character', metadata,
                    Column('literal', Unicode, primary_key=True),
                    Column('grade', Integer),
                    Column('freq', Integer),
                    Column('jlpt', Integer),
                  )
character_l = []
all_l.append([character_l, character.insert()])

stroke_count = Table('stroke_count', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('stroke_count', Integer, nullable=False),
                  )
stroke_count_l = []
all_l.append([stroke_count_l, stroke_count.insert()])

variant = Table('variant', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('variant', String, nullable=False),
                    Column('var_type', String, nullable=False),
                  )
variant_l = []
all_l.append([variant_l, variant.insert()])

rad_name = Table('rad_name', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('rad_name', Unicode, nullable=False),
                  )
rad_name_l = []
all_l.append([rad_name_l, rad_name.insert()])
   
dic_ref = Table('dic_ref', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('dic_ref', String, nullable=False),
                    Column('dr_type', String, nullable=False),
                    Column('m_vol', String, nullable=True),
                    Column('m_page', String, nullable=True),
                  )
dic_ref_l = []
all_l.append([dic_ref_l, dic_ref.insert()])

query_code = Table('query_code', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('q_code', Unicode, nullable=False),
                    Column('qc_type', String, nullable=False),
                    Column('skip_misclass', String, nullable=True),
                  )
query_code_l = []
all_l.append([query_code_l, query_code.insert()])

codepoint = Table('codepoint', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('cp_value', String, nullable=False),
                    Column('cp_type', String, nullable=False)
                  )
codepoint_l = []
all_l.append([codepoint_l, codepoint.insert()])

rad_value = Table('rad_value', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('rad_value', Integer, nullable=False),
                    Column('rad_type', String, nullable=False)
                    )
rad_value_l = []
all_l.append([rad_value_l, rad_value.insert()])

reading = Table('reading', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('reading', Unicode, nullable=False),
                    Column('r_type', String, nullable=False),
                    Column('on_type', String, nullable=True),
                    Column('r_status', String, nullable=True)
                  )
reading_l = []
all_l.append([reading_l, reading.insert()])

meaning = Table('meaning', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('character_literal', Unicode,
                           ForeignKey('character.literal')),
                    Column('meaning', Unicode, nullable=False),
                    Column('m_lang', String, nullable=False)
                  )
meaning_l = []
all_l.append([meaning_l, meaning.insert()])

nanori = Table('nanori', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('character_literal', Unicode,
                        ForeignKey('character.literal')),
                 Column('nanori', Unicode, nullable=False)
                 )
nanori_l = []
all_l.append([nanori_l, nanori.insert()])



def save_all():
    """
    Commit data held in each table_l list.
    Avoid trying to commit an empty list.
    """
    for list in all_l:
        table_l = list[0]
        insert = list[1]
        
        if len(table_l) > n_to_commit:
            conn.execute(insert, table_l)
            del table_l[:]  #empty the list after committing it   

def parse_misc(literal, node):
    grade = None
    freq = None
    jlpt = None
    for m in node:
        if m.tag == "grade":
            grade = m.text
        elif m.tag == "stroke_count":
            stroke_count_l.append({'character_literal':literal,
                                   'stroke_count':m.text})
        elif m.tag == "variant":
            variant_l.append({'character_literal':literal,
                              'variant':m.text,
                              'var_type':m.get("var_type")})
        elif m.tag == "freq":
            freq = m.text
        elif m.tag == "rad_name":
            rad_name_l.append({'character_literal':literal,
                               'rad_name':unicode(m.text)})
        elif m.tag == "jlpt":
            jlpt = m.text
    
    character_l.append({'literal':literal, 
                        'grade':grade,
                        'freq':freq,
                        'jlpt':jlpt})
        
def parse_dic_number(literal, node):
    for d in node:
        dic_ref_l.append({'character_literal':literal,
                          'dic_ref':d.text,
                          'dr_type':d.get("dr_type"),
                          'm_vol':d.get("m_vol"),
                          'm_page':d.get("m_page")})

def parse_query_code(literal, node):
    for q in node:
        query_code_l.append({'character_literal':literal,
                             'q_code':unicode(q.text),
                             'qc_type':q.get("qc_type"),
                             'skip_misclass':q.get("skip_misclass")})
        
def parse_reading_meaning(literal, node):
    for rm in node:
        if rm.tag == "rmgroup":
            for rmg in rm:
                if rmg.tag == "reading":
                    reading_l.append({'character_literal':literal,
                                      'reading':unicode(rmg.text),
                                      'r_type':rmg.get("r_type"),
                                      'on_type':rmg.get("on_type"),
                                      'r_status':rmg.get("r_status")})
                elif rmg.tag == "meaning":
                    m_lang = rmg.get("m_lang")
                    if m_lang is None:
                        m_lang = "en"
                    meaning_l.append({'character_literal':literal,
                                      'meaning':unicode(rmg.text),
                                      'm_lang':m_lang}) 
        elif rm.tag == "nanori":
            nanori_l.append({'character_literal':literal,
                             'nanori':unicode(rm.text)})

def parse_radical(literal, node):
    for r in node:
        rad_value_l.append({'character_literal':literal,
                            'rad_value':r.text,
                            'rad_type':r.get("rad_type")})

def parse_codepoint(literal, node):
    for c in node:
        codepoint_l.append({'character_literal':literal,
                            'cp_value':c.text,
                            'cp_type':c.get("cp_type")})

def fill_database(db_path=None):
    """Fill the supplied database with kanjidic data."""
    
    global conn
                
    engine = create_engine(db_path, echo=False)
    f = open(KANJIDIC2_PATH)    
    metadata.create_all(engine)
    conn = engine.connect()
    
    print "Filling database with KANJIDIC2 data. This takes a while..."
    start = time.time()
    
    #Call save_all() after n_to_save elements. Slight speedup
    n_to_save = 2000
    save_now = 0
    for event, elem in iterparse(f):
        literal = None
        if elem.tag == "character":
            for e in elem:
                if e.tag == "literal":
                    literal = unicode(e.text)
                elif e.tag == "codepoint":
                    parse_codepoint(literal, e)
                elif e.tag == "radical":
                    parse_radical(literal, e)
                elif e.tag == "misc":
                    parse_misc(literal, e)
                elif e.tag == "dic_number": 
                    parse_dic_number(literal, e)
                elif e.tag == "query_code":
                    parse_query_code(literal, e)
                elif e.tag == "reading_meaning":
                    parse_reading_meaning(literal, e)

            save_now += 1
            if save_now > n_to_save:
                save_all()
                save_now = 0
                
            elem.clear() #free memory of no longer needed nodes
    
    #ensure the leftover rows are saved
    save_all()

    print 'Filling database with KANJIDIC2 data took '\
          ' %s seconds' % (time.time() - start)
    print "Done."

    f.close()
    conn.close()
    

def download_dictionary():
    if not os.path.exists(KANJIDIC2_PATH):
        print "kanjidic2.xml not found. Downloading..."
        download.download_kanjidic2()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        db_url = sys.argv[1]
    else:
        db_url = 'sqlite:///kanjidic.sqlite'
        
        print 'Database url not specified. Creating new SQLite database'\
              ', "kanjidic.sqlite", here.'
        
        if os.path.exists('kanjidic.sqlite'):
            print 'Overwriting existing database named kanjidic.sqlite'
            os.remove('kanjidic.sqlite')
    download_dictionary()
    fill_database(db_url)