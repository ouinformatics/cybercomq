#!/usr/bin/env python

from zipfile import ZipFile
from StringIO import StringIO
from urllib2 import urlopen
from cybercom.data.catalog import datalayer as dl
from datetime import datetime
import socket
import os



STATIC_DIR='/static/request'

def catalog_files(commons_id, cat_id, start_date, end_date=None, var_id='URL'):
    ''' Get a list of files from cc catalog based on commons_id and date '''
    return dl.event_results_by_time(commons_id, cat_id, start_date, end_date, var_id)

def getEventResult_Country(**kwargs):
    from xmlrpclib import ServerProxy
    URL = 'http://test.cybercommons.org/dataportal/RPC2/'                
    s = ServerProxy(URL)
    return s.catalog.getEventResult_Country({'cat_id': kwargs['cat_id'], 
            'country': kwargs['country'], 'start_date': kwargs['start_date'],
            'end_date': kwargs['end_date'], 'var_id': kwargs['var_id']} )

def zipurls(files):
    ''' Takes a list of URL locations, fetches files and returns a zipfile ''' 
    if type(files) is list:
        inMemoryOutputFile = StringIO()
        zipFile = ZipFile(inMemoryOutputFile, 'w')
        for filename in files:
            zipFile.writestr(os.path.basename(filename), urlopen(filename).read())
        zipFile.close()
        inMemoryOutputFile.seek(0)
        return inMemoryOutputFile
    else:
        return "ERROR: expected a list of URLs"

def makezip(urls, outname, overwrite=False):
    ''' Make a zipfile from a set of urls '''
    full_path = os.path.join(STATIC_DIR,outname)
    try:    
        if not os.path.exists(outname) and overwrite:
            os.remove(full_path)
        open(full_path, 'w').write(zipurls(urls).read())
        return 'http://%s/%s/%s' % ( socket.gethostname(),'request', outname) 
    except:
        return "Error writing zip file"

    


    

