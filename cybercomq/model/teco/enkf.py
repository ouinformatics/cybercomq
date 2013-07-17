from celery.task import task
from celery.task.sets import subtask
from subprocess import check_call
import glob, shutil 
import os,ast

@task()
def initEnKFrun(callback=None,basedir=None,site=None,siteparam=None):
    if not site:
        raise "site is a required parameter"
    if siteparam:
        param = ast.literal_eval(siteparam)
    else:
        raise "Parameters are required. Please submit Paramerers with task submission"
    if not basedir:
        raise "Basedir is required"
    #Create working directory
    newDir = basedir + "celery_data/" + str(initEnKFrun.request.id)
    check_call(["mkdir",newDir])
    os.chdir(newDir)
    #copy matlab code to working directory
    codeDir =basedir + 'enfk_matlab/'
    for file in glob.glob(codeDir + '*'): 
        shutil.copy(file, newDir) 
    #check_call(["cp","-r",codeDir + '*',newDir])
    #set inital Paramters files
    setup_param(newDir,param)
    if callback:
        result=subtask(callback).delay(task_id=str(initEnKFrun.request.id),wkdir=newDir)
        return {'task_id':result.task_id,'task_name':result.task_name}
    else:
        return newDir


@task()
def runEnKF(task_id=None,wkdir=None):
    os.chdir(wkdir)
    check_call(["/opt/matlab_R2012/bin/matlab","-nodisplay","-r","try,EnKF, catch, end, quit",">","Matlab_log.txt"])
    #clean up output folder
    check_call(["mkdir",wkdir + '/matlab_code' ])
    for file in glob.glob(wkdir + '/*.m'):
        shutil.move(file, wkdir + '/matlab_code')
    for file in glob.glob(wkdir + '/*.txt'):
        if not os.path.basename(file)=='Matlab_log.txt':
            shutil.move(file, wkdir + '/matlab_code')
    for file in glob.glob(wkdir + '/*.csv'):
        shutil.move(file, wkdir + '/matlab_code')
    #scp to static server
    webloc ="/static/queue/model/teco/" + task_id
    check_call(['scp','-r', wkdir , "mstacy@static.cybercommons.org:" + webloc])
    #render results
    temp = "<h4>Result Files</h4><br/>"
    http= "http://static.cybercommons.org/queue/model/teco/" + task_id
    temp = temp +  ' <a href="' + http + '" target="_blank">' + http + '</a><br/>'
    temp = temp + "<h4>EnKF Result Graphs</h4><br/>"
    temp=temp + ' <img src="' + http + '/figure1.jpg" alt="Figure 1">'
    temp=temp + ' <a href="' + http + '/figure1.jpg" target="_blank">' + http + '/figure1.jpg</a><br/>'
    temp=temp + ' <img src="' + http + '/figure2.jpg" alt="Figure 2">'
    temp=temp + ' <a href="' + http + '/figure2.jpg" target="_blank">' + http + '/figure2.jpg</a><br/>'
    temp=temp + ' <img src="' + http + '/figure4.jpg" alt="Figure 4">'
    temp=temp + ' <a href="' + http + '/figure4.jpg" target="_blank">' + http + '/figure4.jpg</a><br/>'
    temp=temp + ' <img src="' + http + '/figure5.jpg" alt="Figure 5">'
    temp=temp + ' <a href="' + http + '/figure5.jpg" target="_blank">' + http + '/figure5.jpg</a><br/>'
    temp=temp + ' <img src="' + http + '/figure7.jpg" alt="Figure 7">'
    temp=temp + ' <a href="' + http + '/figure7.jpg" target="_blank">' + http + '/figure7.jpg</a><br/>'
    temp=temp + ' <img src="' + http + '/figure8.jpg" alt="Figure 8">'
    temp=temp + ' <a href="' + http + '/figure8.jpg" target="_blank">' + http + '/figure8.jpg</a><br/>'
    temp=temp + ' <img src="' + http + '/figure9.jpg" alt="Figure 9">'
    temp=temp + ' <a href="' + http + '/figure9.jpg" target="_blank">' + http + '/figure9.jpg</a><br/>'
    return temp

def setup_param(newDir, param):
    try:
        f1= open(newDir + '/param.m','w')
        tfile ='function x = param(parm)\n% Initial parameters\n'
        tfile=tfile + 'cmin = [' + str(param['cmin1']) + ' ' + str(param['cmin2']) + ' ' + str(param['cmin3']) + ' ' + str(param['cmin4']) + ' '
        tfile=tfile + str(param['cmin5']) + ' ' + str(param['cmin6']) + ' ' + str(param['cmin7']) + ' ' + str(param['cmin8']) + '];\n'
        tfile=tfile + 'cmax = [' + str(param['cmax1']) + ' ' + str(param['cmax2']) + ' ' + str(param['cmax3']) + ' ' + str(param['cmax4']) + ' '
        tfile=tfile + str(param['cmax5']) + ' ' + str(param['cmax6']) + ' ' + str(param['cmax7']) + ' ' + str(param['cmax7']) + '];\n'  
        tfile=tfile + 'x0 = ['+ str(param['x1']) + ' ' + str(param['x2']) + ' ' + str(param['x3']) + ' ' + str(param['x4']) + ' ' + str(param['x5']) + ' '
        tfile=tfile + str(param['x6']) + ' ' + str(param['x7']) + ' ' + str(param['x8']) + '];\n'
        tfile=tfile + 'c0 = ['+ str(param['c1']) + ' ' + str(param['c2']) + ' ' + str(param['c3']) + ' ' + str(param['c4']) + ' ' + str(param['c5']) + ' '
        tfile=tfile + str(param['c6']) + ' ' + str(param['c7']) + ' ' + str(param['c8']) + '];\n'
        tfile=tfile + 'mscut = ' + str(param['mscut']) + ';\n'
        tfile=tfile + 'b = ['+ str(param['b1']) + ' ' + str(param['b2']) + ' ' + str(param['b3']) + ' 0 0 0 0 0]\n'
        tfile=tfile + 'A=[-1             0           0           0           0           0            0           0\n'
        tfile=tfile + '   0             -1           0           0           0           0            0           0\n'
        tfile=tfile + '   0              0          -1           0           0           0            0           0\n'
        tfile=tfile + '   ' + str(param['A41']) + ' ' + str(param['A42']) + ' ' +   '0          -1           0           0            0           0\n'
        tfile=tfile + '   ' + str(param['A51']) + ' ' + str(param['A52']) + ' ' +   '1           0          -1           0            0           0\n'
        tfile=tfile + '   0              0           0        ' + str(param['A64']) + ' ' + str(param['A65']) + ' ' +   '-1 '+ str(param['A67']) + ' ' + str(param['A68']) + '\n'
        tfile=tfile + '   0              0           0           0         ' + str(param['A75']) + ' ' + str(param['A76']) + ' ' +   '-1           0\n'
        tfile=tfile + '   0              0           0           0           0         '+ str(param['A86']) + ' ' + str(param['A87']) + ' ' +   '-1];\n'
        tfile = tfile + "if strcmp(parm,'cmin')\n\tz = cmin;\nend\n"
        tfile = tfile + "if strcmp(parm,'cmax')\n\tz = cmax;\nend\n"
        tfile = tfile + "if strcmp(parm,'x0')\n\tz = x0;\nend\n"
        tfile = tfile + "if strcmp(parm,'c0')\n\tz = c0;\nend\n"
        tfile = tfile + "if strcmp(parm,'mscut')\n\tz = mscut;\nend\n"
        tfile = tfile + "if strcmp(parm,'b')\n\tz = b;\nend\n"
        tfile = tfile + "if strcmp(parm,'A')\n\tz = A;\nend\n"
        tfile = tfile + "x=z;\nend\n"
        
        f1.write(tfile)
        f1.close()
    except Exception as inst:
        raise inst

