# To run on Linux ArcGIS Server:
"""
Xvfb :1 &
export DISPLAY=:1
source /opt/arcgis/server10.0/servercore/.Server/init_server.sh
source /opt/arcgis/server10.0/python26/setenv_python.sh
"""

# Try to avoid multiple arcpy imports as it just slows things down.
try:
    arcpy
except NameError:
    import arcpy

# bring in needed libs
import sys
import os
from os import environ
import tempfile, logging, random, subprocess, shlex
from datetime import datetime, timedelta
from urllib import urlopen
from celery.task import task
import pymongo
import json
import inspect
import nmq


# Set up spatial reference
sr = arcpy.SpatialReference()
sr.factoryCode = 4326
sr.create()
arcpy.env.outputCoordinateSystem = sr

def funcname():
    return inspect.stack()[1][3]
	
def inMemoryPoint(location):
    """
    Create a Feature Set containing a single point location in memory.
    """
    try:
        fc = arcpy.CreateFeatureclass_management("in_memory", "tempfc5", "POINT")
        cur = arcpy.InsertCursor(fc)
        pointArray = arcpy.Array()
        feat = cur.newRow()
        feat.shape = location
        cur.insertRow(feat)
        pointArray.add(location)
        featSet = arcpy.FeatureSet()
        featSet.load(fc)
        return fc
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())


def makePoint(lon,lat, outdir):
    try:
        location = dict(lon=lon,lat=lat)
        logging.info('Creating point...')
        geojson_template = """{ "type": "FeatureCollection", "features": [
        { "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [%(lon)s, %(lat)s]},
          "properties": {"prop0": "value0"}
          } ]
        }"""
        of = open(os.path.join(outdir,'point.json'), 'w')
        of.write(geojson_template % location)
        of.close()
        subprocess.call(['ogr2ogr','-f','ESRI Shapefile', os.path.join(outdir,'point.shp'),os.path.join(outdir,'point.json')])
        logging.info('Point created...')
        return os.path.join(outdir,'point.shp')
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())


def convertRaster(inputf,output,format="IMAGINE Image"):
    """ Convert raster format """
    try:
        logging.info("Converting raster...")
        arcpy.RasterToOtherFormat_conversion(inputf,tempdir,format)
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def circleClip(inputf, location, output, tempdir):
    """ Clip a circle out of a raster """
    try:
        arcpy.env.workspace = tempdir
        size = "50 Kilometers"
        buffer_ring = 'buffer_ring.shp'
        logging.info("Buffering...")
        arcpy.Buffer_analysis(location, buffer_ring, size, "FULL", "ROUND", "NONE")
        logging.info("Clipping...")
        arcpy.Clip_management(inputf, "#", output, buffer_ring, "", "ClippingGeometry")
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())
        
    
def scaleRaster(inputf, output):
    """
    Convert log scaled values to linear scaled values.
    """
    try:
        tmpraster='somefile.img'
        logging.info("Dividing raster...")
        arcpy.gp.Divide_sa(inputf, 10, tmpraster)
        logging.info("Exponentiating...")
        arcpy.gp.Exp10_sa(tmpraster, output)
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())
        
def rasterToPoint(inputf, output):
    """ Convert a raster to vector """
    try:
        tmp_name = hex(random.randint(1,100000)).replace('x','') #random string for file name
        vector_out = os.path.join('/tmp', tmp_name + '.shp')
        logging.info("Converting raster to point... %s %s" %(inputf,vector_out) )
        return arcpy.RasterToPoint_conversion(inputf, vector_out, "VALUE")
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def shp2shp(inputf, output, where=None):
    if where:
        pass

def shp2GeoJSON(inputf, output, where=None):
    try:
        command = ['ogr2ogr','-f','GeoJSON',output,inputf]
        if where:
            command.append('-where %s' % where)
        subprocess.call(command)
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())
    
def hotSpotAnalysis(inputf, output, tempdir, gizscore=None):
    """
    Perform Getis Ord hotspot analysis and optionally threshold output by gizscore,
        including only those points greater than threshold.
    """
    try:
        if gizscore:
            logging.info('Found threshold of %s...' % gizscore)
            tmp_out = 'tmp_' + output
            logging.info('Performing hotspot analysis...')
            arcpy.HotSpotsRendered_stats(inputf, "GRID_CODE", output.replace('.shp','.lyr'), tmp_out, "")
            gizscore_str = '\"GiZScore\" >= %s' % gizscore
            logging.info('Thresholding output...')
            arcpy.FeatureClassToFeatureClass_conversion(tmp_out, tempdir, output.replace('.shp',''), gizscore_str)
            logging.info('Cleaning up...')
            arcpy.Delete_management(tmp_out)
        else:
            logging.info('Performing hostpot analysis...')
            arcpy.HotSpotsRendered_stats(inputf, "GRID_CODE", output.replace('.shp','.lyr'), output, "")
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def gdal2points(filename):
    raster = gdal.Open(filename)
    skip = 1
    srcwin = (0,0,raster.RasterXSize, raster.RasterYSize)
    gt = raster.GetGeoTransform()
    band = raster.GetRasterBand()
    format = '%s %s'
    line = []
    for y in range(srcwin[1], srcwin[1]+srcwin[3], skip):
        data=[]
        band_data=band.ReadAsArray(srcwin[0], y, srcwin[2],1)
        band_data=np.reshape( band_data, (srcwin[2],))
        data = band_data
        for x_i in range(0, srcwin[2],skip):
            x = x_i + srcwin[0]
            geo_x = gt[0] + (x+0.5) * gt[1] + (y+0.5) * gt[2]
            geo_y = gt[3] + (x+0.5) * gt[4] + (y+0.5) * gt[5]
            line.append({"xy":(float(geo_x),float(geo_y)), "val": data[x_i]} )
        return line

def GetisOrd( ):
    pass
    
        
def aggregatePoints(inputf, output, cluster_distance=None):
    """ 
    Aggregate points which are highly spatially autocorrelated
    """
    try:
        if not cluster_distance:
            cluster_distance = "5 Kilometers"
        logging.info('Aggregating points with cluster distance of %s' % cluster_distance)
        tmp_out = 'tmp_aggregate.shp'
        arcpy.AggregatePoints_cartography(inputf, output, cluster_distance)
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def nearAnalysis(inputf, location, search_distance=None):
    """
    Annotate output with near analysis distance from roost site.
    """
    try:
        if not search_distance:
            search_distance = "20 Kilometers"
        arcpy.Near_analysis(inputf, location, search_distance, "NO_LOCATION","ANGLE")
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def zonalStats(inputf,raster):
    """ Run zonal statistics """
    try:
        logging.info("Computing zonal statistics...")
        output = os.path.join(os.path.dirname(inputf),'zonalstats.dbf')
        logging.error('%s %s %s' % (inputf,raster,output))
        os.curdir('/tmp')
        arcpy.gp.ZonalStatisticsAsTable_sa(inputf,"NEAR_FID",raster, output, "DATA","ALL")
        #arcpy.gp.ZonalStatisticsAsTable_sa('/tmp/tmp9RkU25/hotspot_areas.shp',"NEAR_FID",'/tmp/tmp9RkU25/scaled_20100607.010500.img','/tmp/tmp9RkU25/output.dbf',"DATA","ALL")
        os.curdir(os.path.dirname(inputf))
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())


def source(script,update=1):
    """ Source a script in a shell environment""" 
    try:
        pipe = subprocess.Popen('. %s' % script, stdout=subprocess.PIPE, shell=True)
        data = pipe.communicate()[0]
        env = dict((line.split("=", 1) for line in data.splitlines()))
        if update:
            environ.update(env)
        return env
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def geojson2mongo(filename,mongotarget,appendprops=None):
    """
    filename - full path to file
    mongotarget - host/db/collection
    appendprops - a dictionary to append to each feature from the geojson file

    Example:
        geojson2mongo('/tmp/somefile.json', 'fire.rccc.ou.edu/bioscatter/hotspots_test', {'timestamp':'20100505.000000'})
    """
    try:
        host, db, col = mongotarget.split('/')
    except:
        logging.error('Invalid mongotarget, should be formatted as dbhost/db/collection')
    geodict = json.loads(open(filename,'r').read())
    con = pymongo.Connection(host)
    db = con[db]
    col = db[col]
    for item in geodict['features']:
        item['properties'].update(appendprops)
        col.insert(item)
    logging.info('Inserted records into MongoDB: %s' % mongotarget)
    con.close()

def runClustering(timestep, roost="-96.60,33.0", log=True, cleanup=False, task_id=None):
    #arcpy.env.workspace = tempfile.mkdtemp()
    mongotarget = 'fire.rccc.ou.edu/bioscatter/hotspots_test'
    environ['DISPLAY'] = ':600'
    source('/opt/arcgis/server10.0/servercore/.Server/init_server.sh')
    source('/opt/arcgis/server10.0/python26/setenv_python.sh')
    tempdir = tempfile.mkdtemp()
    os.chdir(tempdir)
    unqc_cref = nmq.getScene(timestep, roost, tempdir)
    if log:
        logging.basicConfig(filename=os.path.join(tempdir,'hotspot.log'),
        level=logging.INFO, format='%(asctime)s %(message)s')
    lon,lat = roost.split(',')
    logging.info('Creating point geometry...')
    #location = arcpy.Point(lon,lat)
    #loc = inMemoryPoint(location)
    loc = makePoint(lon,lat,tempdir)
    logging.info('Finished point geometry...')
    unqc_grid = unqc_cref.replace('.tif','.img')
    #convertRaster(unqc_cref,unqc_grid)
    unqc_cref_clipped ='clipped_' + os.path.basename(unqc_cref)
    unqc_cref_scaled = 'scaled_' + os.path.basename(unqc_cref)
    unqc_cref_vect = os.path.join(tempdir, 'points.shp')
    circleClip(unqc_cref, loc, unqc_cref_clipped, tempdir)
    scaleRaster(unqc_cref_clipped, unqc_cref_scaled)
    vect_out = rasterToPoint(unqc_cref_scaled,unqc_cref_vect)
    hotSpotAnalysis(vect_out,'hotspots.shp',tempdir, 10)
    aggregatePoints('hotspots.shp', 'hotspot_areas.shp')
    nearAnalysis('hotspot_areas.shp', loc)
    shp2GeoJSON(os.path.join(tempdir,'hotspot_areas.shp'),os.path.join(tempdir,'hotspot_areas.json'))
    zonalStats(os.path.join(tempdir,'hotspot_areas.shp'), os.path.join(tempdir,unqc_cref_scaled))
    appendprops = {
        'timestep': timestep, 
        'location': roost,
        'task_id': task_id
    }
    geojson2mongo(os.path.join(tempdir,'hotspot_areas.json'), mongotarget, appendprops)
    

def date_range(start_datetime, end_datetime):
    ''' Generator for datetime_ranges at 5 minute intervals '''
    d = start_datetime
    delta = timedelta(minutes=5)
    while d <= end_datetime:
        yield d.strftime('%Y%m%d.%H%M%S')
        d += delta

def runRange(start_time, stop_time, roost, task_id=None):
    start = datetime.strptime(start_time, '%Y%m%d.%H%M%S')
    stop = datetime.strptime(stop_time, '%Y%m%d.%H%M%S')
    for ts in date_range(start,stop):
        logging.info('Processing %s at %s' % (ts,roost))
        try:
            runClustering(ts,roost,task_id=task_id)
        except:
            logging.error('Problem running %s at %s no output will be generated for that timestep' % (ts,roost))
    



if __name__ == '__main__':
    runRange(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
