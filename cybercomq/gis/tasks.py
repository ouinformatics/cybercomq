from celery.task import task
import starspan

@task
def execute_starspan(params, loglocation='mongo'):
    logname = os.tmpnam()
    logfile = open(logname, 'w')
    call(starspancmd(params),stdout=logfile,stderr=subprocess.STDOUT)
