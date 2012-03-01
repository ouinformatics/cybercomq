import hotspots as hs
from celery.task import task
import subprocess

@task
def runRange(start_time, end_time, roost):
    subprocess.call(['/home/arcgis/virtpy/lib/python2.6/site-packages/cybercomq/arcgis/hotspots.sh', start_time, end_time, roost])

@task
def findHotspots(timestep, location):
    hs.runClustering(timestep, location)
