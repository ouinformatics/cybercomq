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
import tempfile, logging, random, subprocess, shlex
from urllib import urlopen

# Set up spatial reference
sr = arcpy.SpatialReference()
sr.factoryCode = 4326
sr.create()
arcpy.env.outputCoordinateSystem = sr

def getVRT(timestep, product, directory):
    """ Request VRT from bioscatter VRT web service """
    url = 'http://test.cybercommons.org/bioscatter/getVrt/%s/%s' % (timestep, product)
    logging.info('Requesting vrt...')
    res = urlopen(url)
    outloc = os.path.join(directory,'%s.vrt' % timestep)
    outfile = open(outloc, 'w')
    outfile.write(res.read())
    outfile.close()
    return outloc

def mkwin(location, rad):
    """ Make a window suitable for "-projwin" in gdal_translate """
    lon, lat = [ int(item) for item in location.split(',') ]
    ulx = lon - rad
    uly = lat + rad
    llx = lon + rad
    lly = lat - rad
    return "%s %s %s %s" % (ulx, uly, llx, lly)

def extractScene(vrtfile,location,radius):
    window = mkwin(location,radius)
    logging.info('Extracting scene from VRT file...')
    command = 'gdal_translate -q -of HFA -projwin %s %s %s' % (window, vrtfile, vrtfile.replace('.vrt','.img'))
    args = command.split(' ')
    subprocess.call(args)
    return vrtfile.replace('.vrt','.img')

def getScene(timestep, location, radius=1.0):
    product = 'unqc_cref'
    vrt = getVRT(timestep, product, arcpy.env.workspace)
    return extractScene(vrt, location, radius)
	
def inMemoryPoint(location):
    """
    Create a Feature Set containing a single point location in memory.
    """
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

def convertRaster(inputf,output,format="IMAGINE Image"):
    """ Convert raster format """
    logging.info("Converting raster...")
    arcpy.RasterToOtherFormat_conversion(inputf,arcpy.env.workspace,format)
    
def circleClip(inputf, location, output):
    """ Clip a circle out of a raster """
    size = "50 Kilometers"
    buffer_ring = 'buffer_ring.shp'
    logging.info("Buffering...")
    arcpy.Buffer_analysis(location, buffer_ring, size, "FULL", "ROUND", "NONE")
    logging.info("Clipping...")
    arcpy.Clip_management(inputf, "#", output, buffer_ring, "", "ClippingGeometry")
    
    
def scaleRaster(inputf, output):
    """
    Convert log scaled values to linear scaled values.
    """
    tmpraster='somefile.img'
    logging.info("Dividing raster...")
    arcpy.gp.Divide_sa(inputf, 10, tmpraster)
    logging.info("Exponentiating...")
    arcpy.gp.Exp10_sa(tmpraster, output)
    
def rasterToPoint(inputf, output):
    """ Convert a raster to vector """
    tmp_name = hex(random.randint(1,100000)).replace('x','') #random string for file name
    vector_out = os.path.join('/tmp', tmp_name + '.shp')
    logging.info("Converting raster to point... %s %s" %(inputf,vector_out) )
    return arcpy.RasterToPoint_conversion(inputf, vector_out, "VALUE")

def shp2shp(inputf, output, where=None):
    if where:
        pass

def shp2GeoJSON(inputf, output, where=None):
    command = ['ogr2ogr','-f','GeoJSON',output,inputf]
    if where:
        command.append('-where %s' % where)
    subprocess.call(command)
    
def hotSpotAnalysis(inputf, output, gizscore=None):
    """
    Perform Getis Ord hotspot analysis and optionally threshold output by gizscore,
        including only those points greater than threshold.
    """
    if gizscore:
        logging.info('Found threshold of %s...' % gizscore)
        tmp_out = 'tmp_' + output
        logging.info('Performing hotspot analysis...')
        arcpy.HotSpotsRendered_stats(inputf, "GRID_CODE", output.replace('.shp','.lyr'), tmp_out, "")
        gizscore_str = '\"GiZScore\" >= %s' % gizscore
        logging.info('Thresholding output...')
        arcpy.FeatureClassToFeatureClass_conversion(tmp_out, arcpy.env.workspace, output.replace('.shp',''), gizscore_str)
        logging.info('Cleaning up...')
        arcpy.Delete_management(tmp_out)
    else:
        logging.info('Performing hostpot analysis...')
        arcpy.HotSpotsRendered_stats(inputf, "GRID_CODE", output.replace('.shp','.lyr'), output, "")
        

def aggregatePoints(inputf, output, cluster_distance=None):
    """ 
    Aggregate points which are highly spatially autocorrelated
    """
    if not cluster_distance:
        cluster_distance = "5 Kilometers"
    logging.info('Aggregating points with cluster distance of %s' % cluster_distance)
    tmp_out = 'tmp_aggregate.shp'
    arcpy.AggregatePoints_cartography(inputf, output, cluster_distance)

def nearAnalysis(inputf, location, search_distance=None):
    """
    Annotate output with near analysis distance from roost site.
    """
    if not search_distance:
        search_distance = "20 Kilometers"
    arcpy.Near_analysis(inputf, location, search_distance, "NO_LOCATION","ANGLE")
    
def runClustering(timestep, roost="-96.60,33.0", log=True):
    arcpy.env.workspace = tempfile.mkdtemp()
    unqc_cref = getScene(timestep, roost)
    if log:
        logging.basicConfig(filename=os.path.join(arcpy.env.workspace,'hotspot.log'),
        level=logging.INFO, format='%(asctime)s %(message)s')
    lon,lat = roost.split(',')
    location = arcpy.Point(lon,lat)
    loc = inMemoryPoint(location)
    unqc_grid = unqc_cref.replace('.tif','.img')
    #convertRaster(unqc_cref,unqc_grid)
    unqc_cref_clipped ='clipped_' + os.path.basename(unqc_cref)
    unqc_cref_scaled = 'scaled_' + os.path.basename(unqc_cref)
    unqc_cref_vect = os.path.join(arcpy.env.workspace, 'points.shp')
    circleClip(unqc_cref, loc, unqc_cref_clipped)
    scaleRaster(unqc_cref_clipped, unqc_cref_scaled)
    vect_out = rasterToPoint(unqc_cref_scaled,unqc_cref_vect)
    hotSpotAnalysis(vect_out,'hotspots.shp',10)
    aggregatePoints('hotspots.shp', 'hotspot_areas.shp')
    nearAnalysis('hotspot_areas.shp', loc)
    shp2GeoJSON(os.path.join(arcpy.env.workspace,'hotspot_areas.shp'),os.path.join(arcpy.env.workspace,'hotspot_areas.json'))

if __name__ == '__main__':
    runClustering(sys.argv[1])
