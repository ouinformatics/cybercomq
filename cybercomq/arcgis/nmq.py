#!/usr/bin/env python
import logging, os, subprocess
from urllib import urlopen

def getVRT(timestep, product, directory):
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

def mkwin(location, rad):
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

def extractScene(vrtfile,location,radius):
    try:
        window = mkwin(location,radius)
        logging.info('Extracting scene from VRT file...')
        command = 'gdal_translate -q -of HFA -projwin %s %s %s' % (window, vrtfile, vrtfile.replace('.vrt','.img'))
        args = command.split(' ')
        subprocess.call(args)
        return vrtfile.replace('.vrt','.img')
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

def getScene(timestep, location, tempdir, product='unqc_cref', radius=1.0):
    try:
        vrt = getVRT(timestep, product, tempdir)
        return extractScene(vrt, location, radius)
    except:
        logging.error("Something happened during %s" % funcname())
        logging.error(sys.exc_info())

