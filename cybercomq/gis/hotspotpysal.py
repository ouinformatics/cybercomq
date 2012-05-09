from osgeo import gdal
from celery.task import task, TaskSet
#from matplotlib import pyplot as plt
from celery.execute import send_task
import pandas
import numpy
from numpy import ma
from pysal.weights.Distance import DistanceBand, Kernel
from pysal.weights.user import adaptive_kernelW
import pysal
from pysal import threshold_binaryW_from_array, knnW
numpy.random.seed(10)
from pysal.esda.getisord import G_Local
from scipy import ndimage
from shapely.geometry import MultiPoint
import time, sys, inspect, json
import geojson
from datetime import datetime, timedelta
import pymongo

def funcname():
    return inspect.stack()[1][3]

con = pymongo.Connection('129.15.41.76')

### Functions to help get radar data from archive ###
import logging, os, subprocess, tempfile, shutil
from urllib import urlopen
logging.basicConfig(level=logging.INFO)
def _getVRT(timestep, product, directory):
    """ Request VRT from bioscatter VRT web service """
    try:
        url = 'http://test.cybercommons.org/bioscatter/getVrt/%s/%s' % (timestep, product)
        logging.info('Requesting vrt...')
        res = urlopen(url)
        outloc = os.path.join(directory,'%s.vrt' % timestep)
        outfile = open(outloc, 'w')
        outfile.write(res.read())
        outfile.close()
        return outloc
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def _mkwin(location, rad):
    """ Make a window suitable for "-projwin" in gdal_translate """
    try:
        lon, lat = [ float(item) for item in location.split(',') ]
        ulx = lon - rad
        uly = lat + rad
        llx = lon + rad
        lly = lat - rad
        return "%s %s %s %s" % (ulx, uly, llx, lly)
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def _extractScene(vrtfile,location,radius):
    """ Use gdal_translate to pull out area of interest from VRT file"""
    try:
        window = _mkwin(location,radius)
        logging.info('Extracting scene from VRT file...')
        command = 'gdal_translate -q -of HFA -projwin %s %s %s' % (window, vrtfile, vrtfile.replace('.vrt','.img'))
        args = command.split(' ')
        subprocess.call(args)
        return vrtfile.replace('.vrt','.img')
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def getScene(timestep, location, tempdir, product='unqc_cref', radius=1.0):
    """ Pull bioscatter data for a given timestep and geographic location
        Example:
        getScene('20100505.000000', '-93.0,33', '/tmp')
    """
    try:
        vrt = _getVRT(timestep, product, tempdir)
        return _extractScene(vrt, location, radius)
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())
### End Functions to help get radar data from archive ###

def gdal2points(filename, datatrans=None):
    ''' Take a GDAL raster and return dictionary containing:
            - 'xy' - xy locations (in raster's coordinate space)
            - 'z' - z value of raster band(1)
            - 'ij' - the corresponing coordinate in the raster pixel grid
    '''
    raster = gdal.Open(filename)
    skip = 1
    srcwin = (0,0,raster.RasterXSize, raster.RasterYSize)
    gt = raster.GetGeoTransform()
    band = raster.GetRasterBand(1)
    format = '%s %s'
    line = []
    for y in range(srcwin[1], srcwin[1]+srcwin[3], skip):
        data=[]
        if datatrans:
            band_data='foo'
        else:
            band_data=band.ReadAsArray(srcwin[0], y, srcwin[2],1)
        band_data=numpy.reshape( band_data, (srcwin[2],))
        data = band_data
        for x_i in range(0, srcwin[2],skip):
            x = x_i + srcwin[0]
            geo_x = gt[0] + (x+0.5) * gt[1] + (y+0.5) * gt[2]
            geo_y = gt[3] + (x+0.5) * gt[4] + (y+0.5) * gt[5]
            line.append({"xy": (float(geo_x),float(geo_y)), "z": data[x_i], 'ij': [x,y]} )
    return line

def applyResults(Zvect, ij_in, mask, outshape):
    Zgrid = numpy.zeros(outshape)
    ij = ma.masked_array(ij_in, mask).compressed()
    for idx, grid in enumerate(ij):
        i, j = grid
        Zgrid[j][i] = Zvect[idx]
    return Zgrid

def GetisOrd(y,locations, distance=5):
    w = pysal.weights.user.threshold_binaryW_from_array(locations,distance)
    return G_Local(y, w, star=True, permutations=None)

def stageData(timestep, location):
    tdir = tempfile.mkdtemp()
    filename = getScene(timestep, location, tdir, radius=0.5)
    return filename 

def cleanup(filename):
    shutil.rmtree(os.path.dirname(filename))

@task
def hotspots(timestep, location, distance=5, zFilter_lt=1, minpixels=5, task_id=None):
    logging.info('Startting...')
    start=time.time()
    filename = stageData(timestep,location)
    raster = gdal.Open(filename)
    rarray = raster.ReadAsArray()
    xyz = pandas.DataFrame(gdal2points(filename))
    logging.info('Cleaning up files...')
    cleanup(filename)
    y = numpy.array(xyz.z)
    y_in = ma.masked_outside( (10**((14.54 + y)/ 10.0)), 60, 2500)
    ij = ma.masked_array(xyz.ij, mask=y_in.mask).compressed()
    data = numpy.array(ij.tolist())
    logging.info('Doing GetisOrd...')
    Z_array = applyResults(GetisOrd(y_in.compressed(),data, distance).Zs, xyz.ij, y_in.mask, rarray.shape)
    Z_array_ma = ma.masked_where(Z_array < zFilter_lt, Z_array)
    group, idx = ndimage.measurements.label(ma.filled(Z_array_ma,0))
    group = ma.masked_where(Z_array < zFilter_lt, group)
    orig_xy = numpy.array(xyz.xy).reshape(rarray.shape)
    outdata = []
    for i in range(idx):
        if idx > 0:
            points = ma.masked_where(group != i, orig_xy)
            values = ma.masked_where(group != i, rarray).compressed()
            zvalues = ma.masked_where(group != i, Z_array).compressed()
            if len(values) >= minpixels:
                stats = {   'n': int(len(values)), 
                            'max': float(values.max()), 
                            'min': float(values.min()), 
                            'mean': float(values.mean()),
                            'std': float(values.std()), 
                            'range': float(values.max() - values.min()), 
                            'sum': float(values.sum()), 
                            'median': float(numpy.median(values)),
                            'zmax': float(zvalues.max()),
                            'zmin': float(zvalues.min()),
                            'zmean': float(zvalues.mean()),
                            'zmedian': float(numpy.median(zvalues)),
                            'zstd': float(zvalues.std()),
                            'tsid': i,
                            'task_id': task_id
                        }
                cvhull = MultiPoint( list(points.compressed())).convex_hull
                stats.update({'timestep':timestep, 'loc': location}) 
                outdoc = json.loads(geojson.dumps(geojson.Feature(id=int(str(timestep.replace('.','')) + str(i).zfill(3)), geometry=json.loads(geojson.dumps(cvhull)), properties=stats)))
                con['bioscatter']['pysal'].insert(outdoc)
    logging.info('It took us %s seconds to process GetisOrd*' % (time.time() - start))
    return 'Success'

def date_range(start_datetime, end_datetime):
    ''' Generator for datetime_ranges at 5 minute intervals '''
    d = start_datetime
    delta = timedelta(minutes=5)
    while d <= end_datetime:
        yield d.strftime('%Y%m%d.%H%M%S')
        d += delta

@task
def hotspotsRange(start_time, stop_time, location, **kwargs):
    start = datetime.strptime(start_time, '%Y%m%d.%H%M%S')
    stop = datetime.strptime(stop_time, '%Y%m%d.%H%M%S')
    kwargs.update({'task_id': hotspotsRange.request.id})
    subtasks = [ send_task("cybercomq.gis.hotspotpysal.hotspots", args=(ts,location), kwargs=kwargs, queue="gis", track_started=True).task_id for ts in date_range(start,stop) ]
    return subtasks  
