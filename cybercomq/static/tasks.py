from celery.task import task
import filezip as fz
from cybercom.data.catalog import datalayer as dl
from datetime import datetime
import socket
import os
import pymongo
from cybercom.data.catalog import dataloader as ddl
from cybercom.data.catalog import datacommons
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
def teco_upload(user_id,filename,file_type='fixed_width',addDict=None,specificOperation=None,seperator=',',skiplines=0,skiplinesAfterHeader=0,match=None):
    try:
        file_name = filename
        db = pymongo.Connection(MONGO_CATALOG_HOST + ":" + str(MONGO_CATALOG_PORT))
        data = db['cybercom_upload']['data'].find_one({'user':user_id})
        taskname='cybercomq.static.tasks.teco_upload'
        #Check and setup catalog data
        if match:
            info ={'taskname':taskname,'file':file_name}
            if not data:
                return {'status':False,'description':'Forcing file is required prior to uploading Observed NEE file.'}
        else:
            info ={'taskname':taskname,'file':file_name}
            #check if file already exists
            if data:
                for item in data['task']:
                    if item['file']==file_name and item['taskname']==taskname:
                        return {'status':False,'description':'Duplicate filename: Filename must be unique, please change the name of file to upload.'} 

        #load into Mongo
        if match:
            addDt = {'user':user_id,'Site':match,'location':match}
        else:
            addDt = {'user':user_id,'Site':file_name,'location':file_name}
        if addDict:
            addDict.update(addDt)
        else:
            addDict = addDt
        #*******set collection***********************************
        if match:
            collection = 'uploaded_nee_data'
        else:
            collection = 'uploaded_data'
        #8*******************************************************
        filename= '/static/cache/test/teco_fileupload/' + filename
        f1 = open(filename,'r')
        header = f1.readline()
        f1.close()
        #check header
        if not "q1" in header:
            if match:
                addDict.update({'q1':1,'q2':1,'q3':1,'q4':1})
            else:
                addDict.update({'q1':1,'q2':1,'q3':1,'q4':1,'q5':1,'q6':1,'q7':1})
        dataload = ddl.Mongo_load('teco',host=MONGO_DATA_HOST)
        dataload.file2mongo(filename,collection,file_type,addDict,calc_obs_date,seperator,skiplines,skiplinesAfterHeader)

        #catalog based on user
        if match:
            if data:
                for item in data['task']:
                    if item['file']==match:
                        item['match']=True
                db['cybercom_upload']['data'].save(data)
            else:
                return {'status':False,'description':'Forcing file is required prior to uploading Observed NEE file.'}
            return {'status':True,'description':'Observed NEE loaded to TECO Data Store. Ready to use in TECO simulations.'}
        else:
            if data:
                data['task'].append(info)
            else:
                data = {'user':user_id,'task':[info]}
            db['cybercom_upload']['data'].save(data)
            #return Status
            return {'status':True,'description':'File loaded to TECO Data Store, please upload Observed NEE file'}
    except Exception as inst:
        #try:
            #db = pymongo.Connection(MONGO_CATALOG_HOST + ":" + str(MONGO_CATALOG_PORT))
            #data = db['cybercom_upload']['data'].find_one({'user':user_id})
        #    info ={'taskname':taskname,'file':file_name,'error':str(inst)}
        #    if data:
        #        data['task'].append(info)
        #    else:
        #        data = {'user':user_id,'task':[info]}
        #    db['cybercom_upload']['data'].save(data)
        #    return {'status':False,'description':str(inst)}
        #except Exception as inst1:
        return {'status':False,'description':"(" + str(inst) + ")" }
def calc_obs_date(row):
    try:
        doy = row["DOY"]
        hour = row["hour"]
        year = row ["Year"]
        dt=datetime(year, 1, 1,hour) + timedelta(doy - 1)
        row['observed_date']=dt
        return row
    except:
        raise
