import geojson
import json, tempfile, shutil, sys, os
import pandas
from subprocess import call
from urllib import urlopen
from zipfile import ZipFile
from StringIO import StringIO


def funcname():
    return inspect.stack()[1][3]

def GeoJSON2shp(inputurl):
    """ Takes the URL of a list of JSON Features and returns a shapefile """
    tempdir = tempfile.mkdtemp()
    output = os.path.join(tempdir,'output.shp')
    try:
        res = urlopen(inputurl)
        inloc = os.path.join(tempdir,'temporary.json')
        infile = open(inloc, 'w')
        jsonout = json.loads(res.read())
        infile.write(geojson.dumps(geojson.FeatureCollection(jsonout)))
        infile.close()
    except:
        logging.error('Had trouble downloading JSON file from: %s' %(inputurl))
    try:
        logging.info('Converting to .shp')
        command = ['ogr2ogr', '-f', 'ESRI Shapefile', output, inloc ]
        call(command)
    except:
        logging.error('''Couldn't run ogr2ogr''')
        logging.error(sys.exc_info())
    try:
        files = [ os.path.join(tempdir, item) for item in ['output.shp','output.prj','output.dbf','output.shx'] ]
        outfile = StringIO()
        zipfile = ZipFile(outfile, 'w')
        logging.info('Zipping...')
        for filename in files:
            zipfile.writestr(os.path.basename(filename), open(filename, 'r').read() )
        zipfile.close()
        outfile.seek()
    except:
        logging.error('''Problem zipping output''')
        logging.error(sys.exec_info())
    logging.info('Cleaning up...')
    shutil.rmtree(tempdir)
    return outfile


def GeoJSONProperties2CSV(inputurl):
    """ Takes the properties/attributes of a GeoJSON document and returns a CSV representation of the data"""
    try:
        res = urlopen(inputurl)
    except:
        logging.error('Had a problem accessing URL: %s' % inputurl)
        logging.error(sys.exec_info())
    try:
        jsonout = json.loads(res.read())
    except:
        logging.error('Had a problem converting json to python, is it well formed?')
        logging.error(sys.exec_info())
    try:
        df = pandas.DataFrame([ item['properties'] for item in jsonout ])
    except:
        logging.error('Had a problem converting to pandas DataFrame')
        logging.error(sys.exec_info())
    try:
        outfile = StringIO()
        df.to_csv(outfile)
        outfile.seek(0)
        outdata = outfile.read()
        return outdata
    except:
        logging.error('Had trouble converting to CSV')
        logging.error(sys.exec_info())
        return None
    
        


