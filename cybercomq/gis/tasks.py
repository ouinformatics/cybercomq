from celery.task import task
import starspan
from subprocess import call, STDOUT
import os

@task
def execute_starspan(loglocation='mongo', **params):
    logname = os.tmpnam()
    logfile = open(logname, 'w')
    call(starspan.starspancmd(**params),stdout=logfile,stderr=STDOUT)
