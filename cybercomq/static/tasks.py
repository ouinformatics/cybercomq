from celery.task import task
import filezip as fz
from cybercom.data.catalog import datalayer as dl
from datetime import datetime
import socket
import os
import pymongo
from cybercom.data.catalog import dataloader as ddl
import ConfigParser

#Retrieve Data Catalog Login 
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
MONGO_CATALOG_HOST= config.get('catalog','host')
MONGO_CATALOG_PORT = config.get('catalog','port')
MONGO_DATA_HOST = config.get('database','host')
DATA_COMMONS='TECO_uploads'
DATA_COLLECTION='data'
#BROKER_URL = "amqplib://jduckles:cybercommons@fire.rccc.ou.edu/cybercom_test"

@task()
def modiscountry(product, country, start_date, end_date, notify=None, outpath=None):
    if socket.gethostname() == 'static.cybercommons.org' and not outpath:
        outpath = '/static/request'
    elif not outpath:
        outpath = os.getcwd()
    cat_ids = fz.catname2catid(product,'807')
    files = []
    # Check to make sure we have date-time
    if isinstance(start_date,datetime) or isinstance(end_date,datetime):
        pass
    else:
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return 'Valid dates are in YYYY-mm-dd format'
    for cat_id in cat_ids:
        files += fz.getEventResult_Country(cat_id=cat_id, 
                        country=country, start_date=start_date, 
                        end_date=end_date, var_id='URL')
    outfile='%s_%s_%s_%s.zip' % (product, country, start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')) 
    download = fz.makezip(files, outname=outfile, outpath=outpath)
    if notify:
        message = """You can download your file at: %s
This link will expire in 48 hours"""  % (download)
        fz.notify_email(notify, "Your MODIS extract for %s %s %s %s has completed" % (product, country, start_date, end_date), message)
    return download

def modistile(product, country, start_date, end_date, outpath=None, notify=None):
    """ Prepare zipfile of a single MODIS tile for download """
    pass
@task()
def teco_upload(user_id,filename,file_type='fixed_width',addDict=None,specificOperation=None,seperator=',',skiplines=0,skiplinesAfterHeader=0):
    try:
        addDt = {'user':user_id}
        if addDict:
            addDict.update(addDt)
        else:
            addDict = addDt
        collection = 'uploaded_data'
        filename= '/static/cache/test/teco_fileupload/' + filename
        dataload = ddl.Mongo_load('teco',host=MONGO_DATA_HOST)
        dataload.file2mongo(filename,collection,file_type,addDict,specificOperation,seperator,skiplines,skiplinesAfterHeader)
        return {'status':True}
    except Exception as inst:
        return {'status':False,'description':str(inst)}
