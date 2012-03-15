import hotspots as hs
from celery.task import task
import subprocess

@task
def runRange(start_time, end_time, roost):
    task_id = str(runRange.request.id)
    subprocess.call(['/home/arcgis/virtpy/lib/python2.6/site-packages/cybercomq/arcgis/hotspots.sh', start_time, end_time, roost,task_id])
    return "http://www.cybercommons.org/app/mongo/db_find/bioscatter/hotspots_test/{'spec':{'properties.task_id':'%s'}}" % task_id

@task
def findHotspots(timestep, location):
    hs.runClustering(timestep, location)
