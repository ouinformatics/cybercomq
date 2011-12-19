from celery import task
from subprocess import call
import shlex 
from pymongo import Connection
import os
from StringIO import StringIO

def starspancmd( doshlex=True, **params ):
    """
    Setup and run a starspan polygon-raster overlay command.
    params = {
    'dbname': 'cybercom',  
    'dbhost': 'fire.rccc.ou.edu',
    'table': 'public.floras',
    'raster': "/ddn_1/data/worldclim/30s/bio_1.tif",
    'outfile': 'tmp.csv',
    'stats': "avg stdev min max sum median",
    'field': "ref_no",
    'geom': '201',
    }
    """
    print params['field']
    if params.has_key('geom'): # If geom supplied as string or list, extract all geometries matching
        if type(params['geom']) is list:
            params['geom'] = str(params['geom']).replace('[','(').replace(']',')')
            params['query'] = "select wkb_geometry, %(field)s from %(table)s where %(field)s in %(geom)s" % (params)
        elif type(params['geom']) in (str,int,float,unicode):
            params['query'] = "select wkb_geometry, %(field)s from %(table)s where %(field)s = %(geom)s" % (params)
    else: # Otherwise select all records and process accordingly
        params['query'] = "select wkb_geometry, %(field)s from %(table)s" % (params)
    command = "/usr/local/starspan/bin/starspan2 --verbose --vector 'PG:dbname=%(dbname)s host=%(dbhost)s tables=%(table)s' --sql '%(query)s' --raster %(raster)s --stats %(outfile)s %(stats)s --fields %(field)s" % (params)
    if doshlex:
        return shlex.split(command) 
    else:
        return command




 
