#!/usr/bin/env python

from zipfile import ZipFile
from StringIO import StringIO
from urllib2 import urlopen
from cybercom.data.catalog import datalayer as dl
from datetime import datetime
import socket
import os


def notify_email(toaddress, subject, bodytext):
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(bodytext)
    msg['Subject'] = subject
    msg['From'] = "DoNotReply@ou.edu"
    msg['To'] = toaddress
    s = smtplib.SMTP('smtp.ou.edu')
    s.sendmail("DoNotReply@ou.edu", [toaddress], msg.as_string())
    return "Notification sent"

def catname2catid(cat_name,commons_id):
    """ For a particular commons_id return cat_ids matching on cat_name """ 
    md = dl.Metadata()
    lookup = md.Search('dt_catalog', ['cat_id', 'cat_name'], where='commons_id = %s' % (commons_id))
    return [ item['cat_id'] for item in lookup if item['cat_name'] == cat_name ]


def catalog_files(commons_id, cat_id, start_date, end_date=None, var_id='URL'):
    """ Get a list of files from cc catalog based on commons_id and date """
    return dl.event_results_by_time(commons_id, cat_id, start_date, end_date, var_id)

def getEventResult_Country(**kwargs):
    from xmlrpclib import ServerProxy
    URL = 'http://test.cybercommons.org/dataportal/RPC2/'                
    s = ServerProxy(URL)
    return s.catalog.getEventResult_Country({'cat_id': kwargs['cat_id'], 
            'country': kwargs['country'], 'start_date': kwargs['start_date'],
            'end_date': kwargs['end_date'], 'var_id': kwargs['var_id']} )

def zipurls(files,out_path):
    ''' Takes a list of URL locations, fetches files and returns a zipfile ''' 
    if type(files) is list:
        OutputFile = open(out_path,'w')
        zipFile = ZipFile(OutputFile, 'w', allowZip64=True)
        for filename in files:
            zipFile.writestr(os.path.basename(filename), urlopen(filename).read())
        zipFile.close()
        OutputFile.seek(0)
        return out_path
    else:
        return "ERROR: expected a list of URLs"

def makezip(urls, outname, outpath, overwrite=False):
    ''' Make a zipfile from a set of urls '''
    full_path = os.path.join(outpath,outname)
    #try:    
    if not os.path.exists(outname) and overwrite:
        os.remove(full_path)
    zipurls(urls,full_path)
    if os.path.exists(full_path):
        return 'http://%s/%s/%s' % ( socket.gethostname(),'request', outname) 
    else:
        return 'Couldn\'t write zipfile'
    #except:
    #    return "Error writing zip file"

    


    

