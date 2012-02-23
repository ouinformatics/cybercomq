import hotspots as hs
from celery.task import task

@task
def findHotspots(timestep, location):
    hs.runClustering(timestep, location)
