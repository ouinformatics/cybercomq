from celery.task import task
import filezip as fz
from cybercom.data.catalog import datalayer as dl
from datetime import datetime
import socket
import os

@task
def modiscountry(product, country, start_date, end_date, outpath=None):
    if socket.gethostname() == 'static.cybercommons.org' and not outpath:
        outpath = '/static/request'
    elif not outpath:
        outpath = os.getcwd()
    cat_ids = fz.catname2catid(product,'807')
    files = []
    for cat_id in cat_ids:
        files += fz.getEventResult_Country(cat_id=cat_id, 
                        country=country, start_date=start_date, 
                        end_date=end_date, var_id='URL')
    outfile='%s_%s_%s_%s.zip' % (product, country, start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')) 
    return fz.makezip(files, outname=outfile, outpath=outpath)

