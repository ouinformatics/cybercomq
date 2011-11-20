from celery.task import task
import socket

@task
def shutdownvm():
    trigger = open('/var/run/shutdown.dat', 'w')
    trigger.close()
    return "Shutting down %s" % (socket.gethostname())
 
    
