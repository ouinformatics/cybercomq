from celery.task import task
#import time 
#from StringIO import StringIO
#from cybercom.api.catalog import datalayerutils as dl
from urllib2 import urlopen
from cybercom.data.catalog import datalayer
from subprocess import call
#call(["ls", "-l"])
import os

if os.uname()[1] == 'ip-129-15-40-58.rccc.ou.edu':
    basedir = '/Users/mstacy/Desktop/TECO_HarvardForest/'
elif os.uname()[1] == 'earth.rccc.ou.edu':
    basedir = '/scratch/cybercom/model/teco/'
#earth_Teco = '/scratch/cybercom/model/teco/runTeco'
#mstacyTeco = '/Users/mstacy/Desktop/TECO_HarvardForest/'#runTeco'

@task(serializer="json")
def add(x, y):
    return x + y
    #md= datalayer.Metadata()
    #return md.Search('dt_location')
    #return x + y 


@task(serilizer="json")
def getTecoinput():
    '''Currently setup up for demo specific input files'''
    try:
        os.chdir(basedir)
        md=datalayer.Metadata()
        sWhere = 'event_id = (select event_id from dt_event where cat_id = %d or cat_id = ) ' % (1446799,1446801) 
        res = md.Search('dt_result',where=sWhere,column=['var_id','result_text'])
        for url in res:
            cmd = "wget "+  url['result_text']
            call([cmd,])
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
        return call([basedir + 'runTeco']) 
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
