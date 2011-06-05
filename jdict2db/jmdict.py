# -*- coding: utf-8 -*-
#Copyright (C) 2011 Houssam Salem <ntsp.gm@gmail.com>
#License: GPLv3; http://www.gnu.org/licenses/gpl.txt

import os
import sys
import time
from xml.etree.cElementTree import iterparse
from sqlalchemy import create_engine, Table, Column, Integer, String, Unicode,\
                       Boolean, ForeignKey, MetaData
import download

JMDICT_PATH = '../data/JMdict'

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


#Set up database tables and required lists.
entry = Table('entry', metadata,
                    Column('ent_seq', Integer, primary_key=True))
entry_l = []
all_l.append([entry_l, entry.insert()])

k_ele = Table('k_ele', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('entry_ent_seq', Integer, ForeignKey('entry.ent_seq')),
                    Column('keb', Unicode, index=True))
k_ele_l = []
all_l.append([k_ele_l, k_ele.insert()])
                  
ke_inf = Table('ke_inf', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('k_ele_id', Integer, ForeignKey('k_ele.id')),
                    Column('ke_inf', String))
ke_inf_l = []
all_l.append([ke_inf_l, ke_inf.insert()])

ke_pri = Table('ke_pri', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('k_ele_id', Integer, ForeignKey('k_ele.id')),
                    Column('ke_pri', String))
ke_pri_l = []
all_l.append([ke_pri_l, ke_pri.insert()])

r_ele = Table('r_ele', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('entry_ent_seq', Integer, ForeignKey('entry.ent_seq'), index=True),
                    Column('reb', Unicode, index=True),
                    Column('re_nokanji', Boolean))
r_ele_l = []
all_l.append([r_ele_l, r_ele.insert()])

re_restr = Table('re_restr', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('r_ele_id', Integer, ForeignKey('r_ele.id'), index=True),
                    Column('keb', Unicode))
re_restr_l = []
all_l.append([re_restr_l, re_restr.insert()])

re_inf = Table('re_inf', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('r_ele_id', Integer, ForeignKey('r_ele.id')),
                    Column('re_inf', Unicode))
re_inf_l = []
all_l.append([re_inf_l, re_inf.insert()])

re_pri = Table('re_pri', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('r_ele_id', Integer, ForeignKey('r_ele.id')),
                    Column('re_pri', String))
re_pri_l = []
all_l.append([re_pri_l, re_pri.insert()])

info = Table('info', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('entry_ent_seq', Integer, ForeignKey('entry.ent_seq')))
info_l = []
all_l.append([info_l, info.insert()])

links = Table('links', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('info_id', Integer, ForeignKey('info.id')),
                    Column('linkag', String),
                    Column('link_desc', String),
                    Column('link_uri', String))
links_l = []
all_l.append([links_l, links.insert()])

bibl = Table('bibl', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('info_id', Integer, ForeignKey('info.id')),
                    Column('bibag', String),
                    Column('bibxt', String))
bibl_l = []
all_l.append([bibl_l, bibl.insert()])

etym = Table('etym', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('info_id', Integer, ForeignKey('info.id')),
                    Column('etym', String))
etym_l = []
all_l.append([etym_l, etym.insert()])

audit = Table('audit', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('info_id', Integer, ForeignKey('info.id')),
                    Column('upd_date', String),
                    Column('upd_detl', String))
audit_l = []
all_l.append([audit_l, audit.insert()])

sense = Table('sense', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('entry_ent_seq', Integer, ForeignKey('entry.ent_seq')))
sense_l = []
all_l.append([sense_l, sense.insert()])

stagk = Table('stagk', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('stagk', Unicode))
stagk_l = []
all_l.append([stagk_l, stagk.insert()])

stagr = Table('stagr', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('stagr', Unicode))
stagr_l = []
all_l.append([stagr_l, stagr.insert()])
 
pos = Table('pos', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('pos', String))
pos_l = []
all_l.append([pos_l, pos.insert()])

xref = Table('xref', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('xref', Unicode))
xref_l = []
all_l.append([xref_l, xref.insert()])

ant = Table('ant', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('ant', Unicode))
ant_l = []
all_l.append([ant_l, ant.insert()])

field = Table('field', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('field', String))
field_l = []
all_l.append([field_l, field.insert()])

misc = Table('misc', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('misc', String))
misc_l = []
all_l.append([misc_l, misc.insert()])

s_inf = Table('s_inf', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('s_inf', Unicode))
s_inf_l = []
all_l.append([s_inf_l, s_inf.insert()])

lsource = Table('lsource', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('lsource', Unicode),
                    Column('lang', String),
                    Column('ls_type', String),
                    Column('ls_wasei', Boolean))
lsource_l = []
all_l.append([lsource_l, lsource.insert()])

dial = Table('dial', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('dial', String))
dial_l = []
all_l.append([dial_l, dial.insert()])

gloss = Table('gloss', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('gloss', Unicode),
                    Column('lang', String),
                    Column('g_gend', String))
gloss_l = []
all_l.append([gloss_l, gloss.insert()])

example = Table('example', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('sense_id', Integer, ForeignKey('sense.id')),
                    Column('example', Unicode))
example_l = []
all_l.append([example_l, example.insert()])

def save_all():
    """
    Commit data held in each table_l list.
    Only commit if the list has more than n_to_commit pending rows.
    """
    for list in all_l:
        table_l = list[0]
        insert = list[1]
        
        if len(table_l) > n_to_commit:
            conn.execute(insert, table_l)
            del table_l[:]  #empty the list after committing it
            
def parse_k_ele(ent_seq, k_ele_pk, node):
    keb = None
    for k in node:
        if k.tag == "keb": 
            keb = k.text
        elif k.tag == "ke_inf":
            ke_inf_l.append({'k_ele_id':k_ele_pk,
                             'ke_inf':k.text})
        elif k.tag == "ke_pri":
            ke_pri_l.append({'k_ele_id':k_ele_pk,
                             'ke_pri':k.text})
    k_ele_l.append({'entry_ent_seq':ent_seq,
                    'keb':keb})

def parse_r_ele(ent_seq, r_ele_pk, node):
    reb = None
    re_nokanji = False
    for r in node:
        if r.tag == "reb":
            reb = unicode(r.text)
        elif r.tag == "re_nokanji":
            re_nokanji = True
        elif r.tag == "re_restr":
            re_restr_l.append({'r_ele_id':r_ele_pk,
                               'keb':unicode(r.text)})
        elif r.tag == "re_inf":
            re_inf_l.append({'r_ele_id':r_ele_pk,
                             're_inf':unicode(r.text)})
        elif r.tag == "re_pri":
            re_pri_l.append({'r_ele_id':r_ele_pk,
                             're_pri':r.text})

    r_ele_l.append({'entry_ent_seq':ent_seq,
                    'reb':reb,
                    're_nokanji':re_nokanji})

def parse_info(ent_seq, info_pk, node):
    for i in node:
        if i.tag == "links":
            link_tag = None
            link_desc = None
            link_uri = None
            for l in i:
                if l.tag == "link_tag":
                    link_tag = l.text
                elif l.tag == "link_desc":
                    link_desc = l.text
                elif l.tag == "link_uri":
                    link_uri = l.text
            links_l.append({'info_id':info_pk,
                            'link_tag':link_tag,
                            'link_desc':link_desc,
                            'link_uri':link_uri})
        elif i.tag == "bibl":
            for b in i:
                bib_tag = None
                bib_txt = None
                if b.tag == "bib_tag":
                    bib_tag = b.text
                elif b.tag == "bib_txt":
                    bib_txt = b.text
            bibl_l.append({'info_id':info_pk,
                           'bib_tag':bib_tag,
                           'bib_txt':bib_txt})
        elif i.tag == "etym":
            etym_l.append({'info_id':info_pk,
                           'etym':i.text})
        elif i.tag == "audit":
            upd_date = None
            upd_detl = None
            for a in i:
                if a.tag == "upd_date":
                    upd_date = a.text
                elif a.tag == "upd_detl":
                    upd_detl = a.text
            audit_l.append({'info_id':info_pk,
                            'upd_date':upd_date,
                            'upd_detl':upd_detl})
    info_l.append({'entry_ent_seq':ent_seq})
        
def parse_sense(ent_seq, sense_pk, node):   
    for s in node:
        if s.tag == "stagk":
            stagk_l.append({'sense_id':sense_pk,
                            'stagk':unicode(s.text)})
        elif s.tag == "stagr":
            stagr_l.append({'sense_id':sense_pk,
                            'stagr':unicode(s.text)})
        elif s.tag == "pos":
            pos_l.append({'sense_id':sense_pk,
                          'pos':s.text})
        elif s.tag == "xref":
            xref_l.append({'sense_id':sense_pk,
                           'xref':unicode(s.text)})
        elif s.tag == "ant":
            ant_l.append({'sense_id':sense_pk,
                          'ant':unicode(s.text)})
        elif s.tag == "field":
            field_l.append({'sense_id':sense_pk,
                            'field':s.text})
        elif s.tag == "misc":
            misc_l.append({'sense_id':sense_pk,
                           'misc':s.text})
        elif s.tag == "s_inf":
            s_inf_l.append({'sense_id':sense_pk,
                            's_inf':unicode(s.text)})
        elif s.tag == "lsource":
            ls_type = s.get("ls_type", 'full')
            ls_wasei = s.get("ls_wasei", False)
            
            #ls_wasei only contains the value 'y' if it exists.
            #but we need boolean values, so set to True
            if ls_wasei is not False:
                ls_wasei = True
                
            #xml:lang expands to that
            lang = s.get("{http://www.w3.org/XML/1998/namespace}lang", 'eng')
            lsource = s.text
            
            #this is necessary because doing unicode(None)
            #returns something that isn't None
            if lsource is not None: 
                lsource = unicode(lsource)
            lsource_l.append({'sense_id':sense_pk,
                              'lsource':lsource,
                              'lang':lang,
                              'ls_type':ls_type,
                              'ls_wasei':ls_wasei})
        elif s.tag == "dial":
            dial_l.append({'sense_id':sense_pk,
                           'dial':s.text})
        elif s.tag == "gloss":
            lang = s.get("{http://www.w3.org/XML/1998/namespace}lang", 'eng')
            g_gend = s.get("g_gend", None)  
            gloss_l.append({'sense_id':sense_pk,
                            'gloss':unicode(s.text),
                            'lang':lang,
                            'g_gend':g_gend})
        elif s.tag == "example":
            example_l.append({'sense_id':sense_pk,
                              'example':unicode(s.text)})
    sense_l.append({'entry_ent_seq': ent_seq})
    
    
def fill_database(db_path):
    """Fill the supplied database with jmdict data."""
    
    global conn

    if not os.path.exists(JMDICT_PATH):
        print "JMdict not found. Downloading..."
        download.download_jmdict()
        
    engine = create_engine(db_path, echo=False)
    f = open(JMDICT_PATH)
    metadata.create_all(engine)
    conn = engine.connect()
    
    print "Filling database with JMdict data. This takes about 40 seconds..."
    start = time.time()
    
    #Primary keys of tables that are used as foreign keys by sub-element
    #tables. The xml file is parsed in document order, so we can be sure
    #the pk matches the sub elements.
    k_ele_pk = 0
    r_ele_pk = 0
    info_pk = 0
    sense_pk = 0
    
    #Call save_all() after n_to_save elements. This shaves off a few seconds.
    n_to_save = 5000
    save_now = 0
    for event, elem in iterparse(f):
        if elem.tag == "entry":
            ent_seq = None
            for e in elem:
                if e.tag == "ent_seq":
                    ent_seq = e.text
                    entry_l.append({'ent_seq':ent_seq})
                elif e.tag == "k_ele":
                    k_ele_pk += 1
                    parse_k_ele(ent_seq, k_ele_pk, e)
                elif e.tag == "r_ele":
                    r_ele_pk += 1
                    parse_r_ele(ent_seq, r_ele_pk, e)
                elif e.tag == "info":
                    info_pk += 1
                    parse_info(ent_seq, info_pk, e)
                elif e.tag == "sense":
                    sense_pk += 1
                    parse_sense(ent_seq, sense_pk, e)
            save_now += 1
            if save_now > n_to_save:
                save_all()
                save_now = 0

            elem.clear() #free memory of no longer needed nodes
    
    #ensure the leftover rows are saved
    global n_to_commit
    n_to_commit = 0
    save_all()
    
    print 'Filling database with JMdict data took '\
          '%s seconds' % (time.time() - start)
    print "Done."
    f.close()
    conn.close()
    
if __name__ == '__main__':
    if len(sys.argv) > 1:
        db_url = sys.argv[1]
        fill_database(db_url)
    else:
        print 'Database url not specified. Creating new SQLite database'\
        ', "jmdict.sqlite", here.'
        if os.path.exists('jmdict.sqlite'):
            print 'Overwriting existing database named jmdict.sqlite'
            os.remove('jmdict.sqlite')
        fill_database('sqlite:///jmdict.sqlite')