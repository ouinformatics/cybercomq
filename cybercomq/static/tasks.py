from celery.task import task
import filezip as fz
from cybercom.data.catalog import datalayer as dl
from datetime import datetime

@task
def modiscountry(product, country, start_date, end_date):
    md = dl.Metadata()
    lookup = md.Search('dt_catalog', ['cat_id', 'cat_name'], where='commons_id = 807')
    cat_ids = [ item['cat_id'] for item in lookup if item['cat_name'] == product ]
    files = []
    for cat_id in cat_ids:
        files += fz.getEventResult_Country(cat_id=cat_id, 
                        country=country, start_date=start_date, 
                        end_date=end_date, var_id='URL')
    outfile='%s_%s_%s_%s.zip' % (product, country, start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')) 
    return fz.makezip(files, outname=outfile)

