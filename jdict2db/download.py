# -*- coding: utf-8 -*-
#Copyright (C) 2011 Houssam Salem <ntsp.gm@gmail.com>
#License: GPLv3; http://www.gnu.org/licenses/gpl.txt

import urllib2
import os
import gzip

JMDICT_URL = 'http://ftp.monash.edu.au/pub/nihongo/JMdict.gz'
KANJIDIC2_URL = 'http://ftp.monash.edu.au/pub/nihongo/kanjidic2.xml.gz'

def download_jmdict():
    """Download and extract JMdict from the Monash FTP server."""
    
    if not os.path.exists('../data/'):
        os.makedirs('../data/')

    #Download and save the compressed file
    print "Downloading JMdict.gz...",
    url = urllib2.urlopen(JMDICT_URL)
    dic_gz = open('../data/JMdict.gz', 'wb')
    dic_gz.write(url.read())
    dic_gz.close()
    print "Done"
    
    #uncompress the file
    print "Extracting...",
    dic_gz = gzip.GzipFile('../data/JMdict.gz')
    dic = open('../data/JMdict', 'wb')
    dic.write(dic_gz.read())
    dic_gz.close()
    dic.close()
    print "Done."
    
    #delete unneeded compressed file
    print "Removing compressed file JMdict.gz...",
    os.remove('../data/JMdict.gz')
    print "Done"
    
    
def download_kanjidic2():
    """Download and extract KANJIDIC2 from the Monash FTP server."""
    
    if not os.path.exists('../data/'):
        os.makedirs('../data/')

    #Download and save the compressed file
    print "Downloading kanjidic2.xml.gz...",
    url = urllib2.urlopen(KANJIDIC2_URL)
    dic_gz = open('../data/kanjidic2.xml.gz', 'wb')
    dic_gz.write(url.read())
    dic_gz.close()
    print "Done"
    
    #uncompress the file
    print "Extracting...",
    dic_gz = gzip.GzipFile('../data/kanjidic2.xml.gz')
    dic = open('../data/kanjidic2.xml', 'wb')
    dic.write(dic_gz.read())
    dic_gz.close()
    dic.close()
    print "Done."
    
    #delete unneeded compressed file
    print "Removing compressed file kanjidic2.xml.gz...",
    os.remove('../data/kanjidic2.xml.gz')
    print "Done"
    

if __name__ == '__main__':    
    download_kanjidic2()
    download_jmdict()