import hotspots as hs
from celery.task import task
import subprocess

@task
def runCall(timestep, roost):
    subprocess.call(['/home/arcgis/virtpy/lib/python2.6/site-packages/cybercomq/arcgis/hotspots.sh', timestep, roost])

@task
def findHotspots(timestep, location):
    hs.runClustering(timestep, location)
