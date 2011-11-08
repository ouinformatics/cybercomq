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
