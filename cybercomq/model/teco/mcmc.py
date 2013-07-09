from celery.task import task
from celery.task.sets import subtask
from subprocess import call

import os,ast

@task()
def initMCMCrun(callback=None,basedir=None,site=None,siteparam=None):
    if not site:
        raise "site is a required parameter"
    if siteparam:
        param = ast.literal_eval(siteparam)
    else:
        raise "Parameters are required. Please submit Paramerers with task submission"
    if not basedir:
        raise "Basedir is required"
    #Create working directory
    newDir = basedir + "celery_data/" + str(initMCMCrun.request.id)
    call(["mkdir",newDir])
    os.chdir(newDir)
    #copy matlab code to working directory
    codeDir =basedir + 'mcmc_matlab/'
    call(["cp","-r",codeDir + '*',newDir])
    #set inital Paramters files
    setup_param(newDir,param)
    if callback:
        result=subtask(callback).delay(task_id=str(initMCMCrun.request.id),wkdir=newDir)
        return {'task_id':result.task_id,'task_name':result.task_name}
    else:
        return newDir


@task()
def runMCMC(task_id=None,wkdir=None):
    os.chdir(wkdir)
    call(["/opt/matlab_R2012/bin/matlab","-nodisplay","-r","try,MCMC, catch, end, quit",">","Matlab_log.txt"])
    webloc ="/static/queue/model/teco/" + task_id
    call(['scp','-r', wkdir , "mstacy@static.cybercommons.org:" + webloc])
    temp = "<h4>Result Files</h4><br/>"
    http= "http://static.cybercommons.org/queue/model/teco/" + task_id
    temp = temp +  ' <a href="' + http + '" target="_blank">' + http + '</a><br/>'
    temp = temp + "<h4>TECO Graphs</h4><br/>"
    temp=temp + ' <a href="' + http + '/figure1.jpg" target="_blank">' + http + '/figure1.jpg</a><br/>'
    temp=temp + ' <a href="' + http + '/figure2.jpg" target="_blank">' + http + '/figure2.jpg</a><br/>'
    return temp

def setup_param(newDir, param):
    try:
        f1= open(newDir + '/param.m','w')
        tfile ='function x = param(parm)\n% Initial parameters\n'
        tfile=tfile + 'nput =' + str(param['nput']) + ';% 2 -- ambient inversion, 3 -- elevated inversion'
        tfile=tfile + 'cmin = [' + str(param['cmin1']) + ' ' + str(param['cmin2']) + ' ' + str(param['cmin3']) + ' ' + str(param['cmin4']) + ' '
        tfile=tfile + str(param['cmin5']) + ' ' + str(param['cmin6']) + ' ' + str(param['cmin7']) + '];\n'
        tfile=tfile + 'cmax = [' + str(param['cmax1']) + ' ' + str(param['cmax2']) + ' ' + str(param['cmax3']) + ' ' + str(param['cmax4']) + ' '
        tfile=tfile + str(param['cmax5']) + ' ' + str(param['cmax6']) + ' ' + str(param['cmax7']) + '];\n'  
        tfile=tfile + 'x0 = ['+ str(param['foliage']) + ' ' + str(param['woody']) + ' ' + str(param['structural_litter']) + ' '+str(param['labile_soil_carbon']) + ' '
        tfile=tfile + str(param['slow_soil_carbon']) + ' ' + str(param['passive_soil_carbon']) + '];\n'
        tfile=tfile + 'nsim = ' + str(param['nsim']) + ';\n'
        tfile = tfile + "if strcmp(parm,'cmin')\n\tz = cmin;\nend\n"
        tfile = tfile + "if strcmp(parm,'cmax')\n\tz = cmax;\nend\n"
        tfile = tfile + "if strcmp(parm,'x0')\n\tz = x0;\nend\n"
        tfile = tfile + "if strcmp(parm,'nput')\n\tz = nput;\nend\n"
        tfile = tfile + "if strcmp(parm,'nsim')\n\tz = nsim;\nend\n"
        tfile = tfile + "x=z;\nend\n"
        
        f1.write(tfile)
        f1.close()
    except:
        raise "Error generating parmater file"
