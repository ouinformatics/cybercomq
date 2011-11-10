from celery.task import task
#import time 
#from StringIO import StringIO
#from cybercom.api.catalog import datalayerutils as dl
from urllib2 import urlopen
from cybercom.data.catalog import datalayer
from subprocess import call
#call(["ls", "-l"])
import os,commands

if os.uname()[1] == 'ip-129-15-40-58.rccc.ou.edu':
    basedir = '/Users/mstacy/Desktop/TECO_HarvardForest/'
elif os.uname()[1] == 'earth.rccc.ou.edu':
    basedir = '/scratch/cybercom/model/teco/'

@task(serializer="json")
def add(x, y):
    return x + y

@task(serilizer="json")
def getTecoinput():
    '''Currently setup up for demo specific input files'''
    try:
        md=datalayer.Metadata()
        sWhere = "var_id = 'URL' and event_id in (select event_id from dt_event where cat_id = %d or cat_id = %d) " % (1446799,1446801) 
        res = md.Search('dt_result',where=sWhere,column=['var_id','result_text'])
        for url in res:
            temp= url['result_text'].split("/")
            fname = temp[len(temp)-1]
            filepath = basedir + fname
            a = urlopen(url['result_text'])
            f1= open(filepath,'w')
            f1.write(a.read())
        return True 
    except:
        raise
@task
def runTeco(param):
    ''' run teco model 
        param = {url to files files required to run model}
    '''
    try:
        os.chdir(basedir)
        call([basedir + 'runTeco'])
        return 'TECO Model run Complete'
    except:
        raise

@task
def getLocation(commons_id=None):
    md=datalayer.Metadata()
    if commons_id != None:
        whr = 'commons_id = %d' % (commons_id)
        return md.Search('dt_location',where = whr)
    return md.Search('dt_location')

@task
def sleep(s):
    time.sleep(s)
    return None
