from celery.task import task
from celery.task.sets import subtask
from pymongo import Connection
from datetime import datetime,timedelta
from urllib2 import urlopen
from cybercom.data.catalog import datalayer,dataloader
from subprocess import call,STDOUT
import os,commands,json,ast
import math
mongoHost = 'fire.rccc.ou.edu'
if os.uname()[1] == 'dhcp-213-41.rccc.ou.edu':
    basedir = '/Users/mstacy/Desktop/projects/TECO_HarvardForest/'
elif os.uname()[1] == 'Marks-MacBook-Pro.local':
    basedir = '/Users/mstacy/Desktop/projects/TECO_HarvardForest/'
elif os.uname()[1] == 'earth.rccc.ou.edu':
    basedir = '/scratch/cybercom/model/teco/'
#elif os.uname()[1] == 'static.cybercommons.org':
#    basedir =raise "Current server( %s ) doesn't have directory for TECO Model setup!" % os.uname()[1]

@task()
def add(x, y,callback=None):
    result = x + y
    if callback is not None:
        subtask(callback).delay(result)
    return result
@task()
def runTECOworkflow(site=None,base_yrs=None,forecast=None,siteparam=None,mod_weather=None,model='TECO_f1',dda_freq=1 ,upload=None, **kwargs):
    if model == 'TECO_f1':
        if siteparam:
            result=initTECOrun.delay(site=site,base_yrs=base_yrs,forecast=forecast,siteparam=siteparam,mod_weather=mod_weather,upload=upload,callback=subtask(runTeco))
            return {'task_id':result.task_id,'task_name':result.task_name}
        else:
            result=initTECOrun.delay(site=site,base_yrs=base_yrs,forecast=forecast,mod_weather=mod_weather,upload=upload,callback=subtask(runTeco))
            return {'task_id':result.task_id,'task_name':result.task_name}
    elif model == 'DDA':
        dda=1
        if not dda_freq:
            raise "Please specify parameter 'dda_hour_freq', Frequency to pass to kalmen filter(1 for every hour, 8 for every eighth hour)"
        if siteparam:
            result=initTECOrun.delay(site=site,base_yrs=base_yrs,forecast=forecast,siteparam=siteparam,mod_weather=mod_weather,model=model,dda_freq=dda_freq,upload=upload,callback=subtask(runTeco))
            return {'task_id':result.task_id,'task_name':result.task_name}
        else:
            result=initTECOrun.delay(site=site,base_yrs=base_yrs,forecast=forecast,mod_weather=mod_weather,model=model,dda_freq=dda_freq,upload=upload,callback=subtask(runTeco))
            return {'task_id':result.task_id,'task_name':result.task_name}
    elif model =='grassland':
        result=initTECOrun.delay(site=site,base_yrs=base_yrs,forecast=forecast,siteparam=siteparam,mod_weather=mod_weather,model=model,upload=upload,callback=subtask(runTeco))
        return {'task_id':result.task_id,'task_name':result.task_name}    
    else:
        raise "Model parameter must be either TECO_f1 or DDA"
@task()
def initTECOrun(callback=None,**kwargs):
    ''' Create working directory
        Create data files
        Link executable to file
        return working directory
    '''
    try:
        if 'upload' in kwargs:
            upload = kwargs['upload']
        else:
            upload=None
        if 'site' in kwargs: 
            site = kwargs['site']
        else:
            raise "site is a required parameter"
        if 'base_yrs' in kwargs:
            base_yrs = kwargs['base_yrs']
        else:
            raise "base_yrs is a required parameter"
        if 'forecast' in kwargs:
            forecast = kwargs['forecast']
        else:
            raise "forecast is required parameter(list with tuple of year forcast and year getting data '[(2007,1991)]')"
        if 'siteparam' in kwargs:
            param = ast.literal_eval(kwargs['siteparam'])
        else:
            #Use default siteparam for site
            param = json.loads(urlopen("http://test.cybercommons.org/mongo/db_find/teco/siteparam/{'spec':{'site':'" + site + "'}}/").read())[0]
        if 'mod_weather' in kwargs:
            try:
                modWeather = ast.literal_eval(kwargs['mod_weather'])
            except:
                modWeather={}
        else:
            modWeather={}
        if 'model' in kwargs:
            model=kwargs['model']
            if model=='grassland':
                model='grassland'
                dda_freq=None
            else:
                if 'dda_freq' in kwargs:
                    dda_freq=kwargs['dda_freq']
                else:
                    dda_freq=1
        else:
            model='TECO_f1'
            dda_freq=None
        newDir = basedir + "celery_data/" + str(initTECOrun.request.id)
        call(["mkdir",newDir])
        os.chdir(newDir)
        #create link to teco executable
        if model=='TECO_f1':
            call(["ln","-s",basedir + "runTeco",newDir + "/runTeco"])
        elif model=='DDA':
            call(["ln","-s",basedir + "prod_ver/runTeco_dda",newDir + "/runTeco"])
        elif model=='grassland':
            call(["ln","-s",basedir + "grass_ver/grassTECO",newDir + "/runTeco"])
        else:
            raise "Model does not exist. Please set Model available (TECO_f1,DDA,grassland)" 
        #Set paramater file
        if model=='grassland':
            set_site_param_grass(initTECOrun.request.id,param)
            call(["ln","-s",basedir + "grass_ver/TECO_amb_h.txt",newDir + "/forcing.txt"])
        else:
            set_site_param(initTECOrun.request.id,param)
            custom_tecov2_setup(initTECOrun.request.id,site,param['inputfile'],base_yrs, forecast,modWeather,upload)

            #Set NEE data for model DDA and Legacy TECO Model
            if upload:
                custom_tecov2_nee(initTECOrun.request.id,site,param['NEEfile'],base_yrs, forecast,upload)
            else:
                if site == 'US-HA1':
                    custom_tecov2_nee(initTECOrun.request.id,site,param['NEEfile'],base_yrs, forecast,upload)

        if callback:
            result=subtask(callback).delay(task_id=str(initTECOrun.request.id),model=model,dda_freq=dda_freq)
            return {'task_id':result.task_id,'task_name':result.task_name}
        else:
            return newDir
    except:
        raise

@task()#serializer="json")
def getLocations(**kwargs):
    db = Connection(mongoHost)
    #check if site in siteparams
    siteparam = db['teco']['siteparam'].distinct('site')
    #check if site in forcing data
    forcing = db['teco']['forcing'].distinct('Site')
    sites=[]
    for site in forcing:
        if site in siteparam:
            sites.append(site)
    findloc=[]
    #check if catalog location metadata
    for row in sites:
        for rr in  db['catalog']['location'].find({'loc_id':row}):
            findloc.append(row)
    return findloc

@task()
def runTeco(task_id=None,model=None, dda_freq=1 ,**kwargs):#runDir):
    ''' run teco model 
        param = {url to files files required to run model}
    '''
    try:
        if 'task_id' == None:
            raise "'task_id' from cybercomq.model.teco.task.initTECOrun not given in keyword arguments."
        #runloc = os.path.join(runDir,'runTeco')
        wkdir =basedir + "celery_data/" + task_id
        os.chdir(wkdir)
        logfile= open(wkdir + "/logfile.txt","w")
        if model == None or model == 'TECO_f1':
            call(["./runTeco", wkdir + "/sitepara_tcs.txt", wkdir + "/Results.txt"],stdout=logfile,stderr=STDOUT)
        elif model=='DDA':
            call(["./runTeco", wkdir + "/sitepara_tcs.txt", wkdir + "/Results.txt","1", str(dda_freq)],stdout=logfile,stderr=STDOUT)
        elif model =='grassland':
            call(["./runTeco", wkdir + "/sitepara_tcs.txt", wkdir + "/forcing.txt"],stdout=logfile,stderr=STDOUT)
            
        call(['rm',wkdir + '/runTeco'])
        #call(['rm',wkdir + '/HarvardForest_hr_Chuixiang.txt'])
        #call(['./runTeco',wkdir + "/sitepara_tcs.txt",wkdr + "/US-HA1_TECO_04.txt"])

       # webloc ="/static/queue/model/teco/" + task_id + ".txt"
        webloc ="/static/queue/model/teco/" + task_id
       # call(['scp', wkdir +"/US-HA1_TECO_04.txt", "mstacy@static.cybercommons.org:" + webloc])
        call(['scp','-r', wkdir , "mstacy@static.cybercommons.org:" + webloc])
        #load to mongo
        if model!= 'grassland':
            dld = dataloader.Mongo_load('teco',host = mongoHost )
            collection='taskresults'
            adddict ={'task_id': task_id}
            dld.file2mongo(wkdir + "/Results.txt",collection,file_type='fixed_width',addDict=adddict,specificOperation=set_observed_date)


        #http= "http://static.cybercommons.org/queue/model/teco/" + task_id + ".txt"
        temp = "<h5>Result Files</h5><br/>"
        http= "http://static.cybercommons.org/queue/model/teco/" + task_id
        temp = temp +  ' <a href="' + http + '" target="_blank">' + http + '</a><br/>'
        if model!= 'grassland':
            temp = temp + "<h5>TECO Graphs</h5><br/>"
            http= "http://static.cybercommons.org/apptest/teco_plot/?task_id=" + task_id 
            temp = temp +  ' <a href="' + http + '" target="_blank">' + http + '</a><br/>'
        #temp = temp + "<br/><h5>Graph currently under Construction</h5>"
        #http= "http://static.cybercommons.org/queue/model/teco/" + task_id
        return temp #http #'TECO Model run Complete'
    except:
        raise
@task()
def load_teco_upload(filename,user,db='teco',collection='user_forcing'):
    dld = dataloader.Mongo_load(db,host = mongoHost)
    try:
        name=os.path.splitext(os.path.basename(filename))[0]
        adddict ={'user':user,'Site':name}
        dld.file2mongo(filename,collection,file_type='fixed_width',addDict=adddict)#,specificOperation=set_observed_date)
    except:
        raise

def set_observed_date(row):
    odate = datetime(int(row['year']),1,1,int(row['hour'])-1,0,0)
    doy = timedelta(days=int(row['doy'])-1)
    observed_date = odate + doy
    row['observed_date']=observed_date
    row['week']=observed_date.isocalendar()[1]
    row['month']=observed_date.month
    row['day']=row['doy']
    return row
def set_site_param_grass(task_id,param):
    head=['Lat','Co2ca','output','newline','a1','Ds0','Vcmx0','extku','xfang','alpha','stom_n','newline','Wsmax','Wsmin','newline',
          'rdepth','rfibre','newline','SLA','LAIMAX','LAIMIN','newline','Rootmax','Stemmax','SenS','SenR','newline']
    wkdir =basedir + "celery_data/" + str(task_id)
    os.chdir(wkdir)
    header=''
    value=''
    f1 = open('sitepara_tcs.txt','w')
    for col in head:
        if col =='newline':
            f1.write(header + '\n') 
            f1.write(value + '\n')
            header=''
            value=''
        else:
            header = header + col + "\t"
            value = value + str(param[col]) + "\t"
    f1.close()
        
def set_site_param(task_id,param):
    ''' Param is a dictionary with the site paramiters'''
    head =[ 'site','vegtype','inputfile','NEEfile','outputfile','lat','Longitude','wsmax','wsmin','gddonset',
            'LAIMAX','LAIMIN','rdepth','Rootmax','Stemmax','SapR','SapS','SLA','GLmax','GRmax','Gsmax','stom_n',
            'a1','Ds0','Vcmx0','extkU','xfang','alpha','co2ca','Tau_Leaf','Tau_Wood','Tau_Root','Tau_F','Tau_C',
            'Tau_Micro','Tau_SlowSOM','Tau_Passive']
    inithead =[ 'wsmax','wsmin','gddonset','LAIMAX','LAIMIN','rdepth','Rootmax','Stemmax','SapR','SapS','SLA','GLmax',
            'GRmax','Gsmax','a1','Ds0','Vcmx0','alpha','Tau_Leaf','Tau_Wood','Tau_Root','Tau_F','Tau_C',
            'Tau_Micro','Tau_SlowSOM','Tau_Passive','TminV','TmaxV','ToptV','Tcold','Gamma_Wmax','Gamma_Tmax']
    carboncol=['nsc','Q_leaf','Q_wood','Q_root1','Q_root2','Q_root3','Q_coarse','Q_fine','Q_micr','Q_slow','Q_pass','S_w_min','Q10_h']
    #addInitfile = ['TminV','TmaxV','ToptV','Tcold','Gamma_Wmax','Gamma_Tmax']
    #workaround ="-6.3833\n47.934\n32.963\n10.733\n0.00015\n0.00161\n"#0.51041\n"
    wkdir =basedir + "celery_data/" + str(task_id)
    os.chdir(wkdir)
    header =''
    value=''
    initvalue=''
    for col in head:
        if col =="Tau_Passive":
            header = header + col + "\n"
            value = value + str(param[col]) + "\n"
        else:
            header = header + col + "\t"
            value = value + str(param[col]) + "\t"
    f1 = open('sitepara_tcs.txt','w')
    f1.write(header)
    f1.write(value)
    f1.close()
    for col in inithead:
        initvalue= initvalue + str(param[col]) + "\n"
    try:
        for col in carboncol:
            initvalue= initvalue + str(param[col]) + "\n"
    except:
        pass
    f2 = open('initial_opt.txt','w')
    f2.write(initvalue)
    #f2.write(workaround)
    f2.close()
def custom_tecov2_setup(task_id,site,filename,years,forecast,modWeather,upload):
    # Header row
    header='Year  DOY  hour  T_air q1   Q_air  q2   Wind_speed q3     Precip   q4   Pressure   q5  R_global_in q6   R_longwave_in q7   CO2'
    head =['Year','DOY','hour','T_air','q1','Q_air','q2','Wind_speed','q3','Precip','q4','Pressure','q5',
            'R_global_in','q6','R_longwave_in','q7','CO2']
    #fixed width list of values
    wd=[4,5,7,14,2,14,2,14,2,14,2,14,2,14,2,14,2,11]
    #set working diectory
    wkdir = basedir + "celery_data/" + str(task_id)
    #wkdir = "/home/mstacy/test"
    os.chdir(wkdir)
    #open file and set header
    outfile = open(filename,"w")
    outfile.write(header + '\n\n')
    #open mongo connection
    db = Connection(mongoHost).teco
    #safe eval to get start and end dates
    yr=ast.literal_eval(years)
    start = datetime(yr[0],1,1)
    end = datetime(yr[1] + 1,1,1)
    #figure time step currrently only working for Hourly and half hourly
    if upload:
        stepRes=db.uploaded_data.find({"Site":site}).sort([('observed_date',1),('hour',1)]).limit(2)
    else:
        stepRes=db.forcing.find({"Site":site}).sort([('observed_date',1),('hour',1)]).limit(2)
    step=stepRes[1]['hour']-stepRes[0]['hour']
    if step == 0.5:
        stepdenom=2
    else:
        stepdenom=1
    #safe eval forecast to list of tuples
    forc = ast.literal_eval(forecast)
    set_input_data(db,site,head,wd,outfile,start,end,forc,stepdenom,modWeather,upload)
def custom_tecov2_nee(task_id,site,filename,years,forecast,upload):#,modWeather):
    # Header row
    header='Year DOY hour     T_air q1    precip q2       PAR q3     R_net q4    ustar         NEE filled_NEE         LE  filled_LE\n'
    head =['Year','DOY','hour','T_air','q1','precip','q2','PAR','q3','R_net','q4','ustar','NEE','filled_NEE','LE','filled_LE']
    #fixed width list of values
    wd=[4,4,5,11,2,11,2,11,2,11,2,11,11,11,11,11]
    #set working diectory
    wkdir = basedir + "celery_data/" + str(task_id)
    #wkdir = "/home/mstacy/test"
    os.chdir(wkdir)
    #open file and set header
    outfile = open(filename,"w")
    outfile.write(header)
    #open mongo connection
    db = Connection(mongoHost).teco
    #safe eval to get start and end dates
    yr=ast.literal_eval(years)
    start = yr[0]
    end = yr[1] 
    #figure time step currrently only working for Hourly and half hourly
    #stepRes=db.forcing.find({"Site":site}).sort([('observed_date',1),('hour',1)]).limit(2)
    #step=stepRes[1]['hour']-stepRes[0]['hour']
    #if step == 0.5:
    #    stepdenom=2
    #else:
    #stepdenom=1
    #safe eval forecast to list of tuples
    forc = ast.literal_eval(forecast)
    set_nee_data(db,site,head,wd,outfile,start,end,forc,upload)#,stepdenom,modWeather)
def set_nee_data(db,site,fields,wd,outfile,startyr,endyr,forc,upload,divby=1,modWeather={}):
    if upload:
        result = db.uploaded_nee_data.find({"Site":site,'user':upload,"Year":{"$gte": startyr, "$lte": endyr}}).sort([('Year',1),('DOY',1),('hour',1)])
    else:
        result = db.observed_nee.find({"Site":site,"Year":{"$gte": startyr, "$lte": endyr}}).sort([('Year',1),('DOY',1),('hour',1)])
    for row in result:
        if row['hour'] == math.ceil(row['hour']):
            rw=''
            for col in fields:
                if col =='Year' or col == 'DOY':
                    rw = rw +  str(modify_weather(int(row[col]),col,modWeather)).rjust(int(wd[fields.index(col)]),' ')
                else:
                    rw = rw +  str(modify_weather(row[col],col,modWeather)).rjust(int(wd[fields.index(col)]),' ')
                #if col == 'Precip':
                #    rw = rw +  str(modify_weather(((row[col] + halfPrecip)/divby),col,modWeather)).rjust(int(wd[fields.index(col)]),' ')
                #else:
                #rw = rw +  str(modify_weather(row[col],col,modWeather)).rjust(int(wd[fields.index(col)]),' ')
            outfile.write(rw + '\n')
            #halfPrecip=0.0
        else:
            print "wrong way"
            #halfPrecip=row['Precip']
    #forecast added to add to forcing file
    for forc_yr in forc:
        f0=isLeap(forc_yr[0])
        f1=isLeap(forc_yr[1])
        opt=0
        if f0==f1:
            opt=1
        elif f0:
            opt=2
        else:
            opt=3
        #halfPrecip=0.0
        if upload:
            result= db.uploaded_nee_data.find({'Site':site,'user':upload,'Year':forc_yr[1]}).sort([('DOY',1),('hour',1)])
        else:
            result= db.observed_nee.find({'Site':site,'Year':forc_yr[1]}).sort([('DOY',1),('hour',1)])
        for row in result:
            if row['hour']== math.ceil(row['hour']):
                if opt==1:
                    fw_file(outfile,fields,wd,forc_yr[0],int(row['DOY']),row,0.0,divby,modWeather)
                elif opt==2:
                    if row['DOY']>= 60:
                        if row['DOY'] == 60 and row['hour'] == 0.0:
                            if upload:
                                result228 = db.uploaded_nee_data.find({'Site':site,'user':upload,'Year':forc_yr[1],'DOY':59}).sort([('hour',1)])
                            else:
                                result228 = db.observed_nee.find({'Site':site,'Year':forc_yr[1],'DOY':59}).sort([('hour',1)])
                            for row28 in result228:
                                fw_file(outfile,fields,wd,forc_yr[0],60,row28,0.0,divby,modWeather)
                        fw_file(outfile,fields,wd,forc_yr[0],int(row['DOY'])+1,row,0.0,divby,modWeather)
                    else:
                        fw_file(outfile,fields,wd,forc_yr[0],int(row['DOY']),row,0.0,divby,modWeather)
                else:
                    if row['DOY']>= 60:
                        if row['DOY'] == 60:
                            pass
                        else:
                            fw_file(outfile,fields,wd,forc_yr[0],int(row['DOY'])-1,row,0.0,divby,modWeather)
                    else:
                        fw_file(outfile,fields,wd,forc_yr[0],int(row['DOY']),row,0.0,divby,modWeather)
                #halfPrecip=0.0
            else:
                print "forcasting wrong way"
                #halfPrecip=row['Precip'] 

def set_input_data(db,site,fields,wd,outfile,start,end,forc,divby,modWeather,upload):
    #Set result set from mongo
    halfPrecip=0.0
    if upload:
        result = db.uploaded_data.find({"Site":site,'user':upload,"observed_date":{"$gte": start, "$lt": end}}).sort([('observed_date',1)])
    else:
        result = db.forcing.find({"Site":site,"observed_date":{"$gte": start, "$lt": end}}).sort([('observed_date',1)])
    for row in result:
        if row['hour'] == math.ceil(row['hour']):
            rw=''
            for col in fields:
                if col == 'Precip':
                    rw = rw +  str(modify_weather(((row[col] + halfPrecip)/divby),col,modWeather)).rjust(int(wd[fields.index(col)]),' ')
                else:
                    rw = rw +  str(modify_weather(row[col],col,modWeather)).rjust(int(wd[fields.index(col)]),' ')
            outfile.write(rw + '\n')
            halfPrecip=0.0
        else:
            halfPrecip=row['Precip']
    #forecast added to add to forcing file
    for forc_yr in forc:
        f0=isLeap(forc_yr[0])
        f1=isLeap(forc_yr[1])
        opt=0
        if f0==f1:
            opt=1
        elif f0:
            opt=2
        else:
            opt=3
        halfPrecip=0.0
        if upload:
            result= db.uploaded_data.find({'Site':site,'user':upload,'Year':forc_yr[1]}).sort([('observed_date',1)])
        else:
            result= db.forcing.find({'Site':site,'Year':forc_yr[1]}).sort([('observed_date',1)])
        for row in result:
            if row['hour']== math.ceil(row['hour']):
                if opt==1:
                    fw_file(outfile,fields,wd,forc_yr[0],row['DOY'],row,halfPrecip,divby,modWeather)
                elif opt==2:
                    if row['DOY']>= 60:
                        if row['DOY'] == 60 and row['hour'] == 0.0:
                            if upload:
                                result228 = db.uploaded_data.find({'Site':site,'user':upload,'Year':forc_yr[1],'DOY':59}).sort([('observed_date',1)])
                            else:
                                result228 = db.forcing.find({'Site':site,'Year':forc_yr[1],'DOY':59}).sort([('observed_date',1)])
                            for row28 in result228:
                                fw_file(outfile,fields,wd,forc_yr[0],60,row28,halfPrecip,divby,modWeather)
                        fw_file(outfile,fields,wd,forc_yr[0],row['DOY']+1,row,halfPrecip,divby,modWeather)
                    else:
                        fw_file(outfile,fields,wd,forc_yr[0],row['DOY'],row,halfPrecip,divby,modWeather)
                else:
                    if row['DOY']>= 60:
                        if row['DOY'] == 60:
                            pass
                        else:
                            fw_file(outfile,fields,wd,forc_yr[0],row['DOY']-1,row,halfPrecip,divby,modWeather)
                    else:
                        fw_file(outfile,fields,wd,forc_yr[0],row['DOY'],row,halfPrecip,divby,modWeather)
                halfPrecip=0.0
            else:
                halfPrecip=row['Precip']
            #lastrow=row
            #rw=''
            #for col in fields:#head:
            #    if col =='Year':
            #        rw = rw +  str(forc_yr[0]).rjust(int(wd[fields.index(col)]),' ')
            #    else:
            #        rw = rw +  str(row[col]).rjust(int(wd[fields.index(col)]),' ')
            #outfile.write(rw + '\n')
def fw_file(outfile,fields,wd,Year,DOY,row,halfPrecip,divby,modWeather):
    rw=''
    for col in fields:
        if col =='Year':
            rw = rw +  str(Year).rjust(int(wd[fields.index(col)]),' ')
        elif col == 'DOY':
            rw = rw +  str(DOY).rjust(int(wd[fields.index(col)]),' ')
        elif col =='hour':
            rw = rw +  str(row[col]).rjust(int(wd[fields.index(col)]),' ')
        elif col == 'Precip':
            rw = rw +  str(modify_weather((row[col] + halfPrecip)/divby ,col ,modWeather)).rjust(int(wd[fields.index(col)]),' ')
        else:
            rw = rw +  str(modify_weather(row[col],col,modWeather)).rjust(int(wd[fields.index(col)]),' ')
    outfile.write(rw + '\n')
def modify_weather(value,col,modWeather):
    modvalue = value
    if col in modWeather:
        modify=modWeather[col]
        for mods in modify:
            if mods[0]=='+' or mods[0]=='add':
                modvalue = modvalue + mods[1]
            elif mods[0]=='*' or mods[0]=='x' or mods[0]=='multiply':
                modvalue = modvalue * mods[1]
            elif mods[0]=='-' or mods[0]=='subtract':
                modvalue = modvalue - mods[1]
            elif mods[0]=='divide' or mods[0]=='/':
                try:
                    modvalue = modvalue / mods[1]
                except:
                    pass
    return modvalue

def isLeap(year):
    if (year % 4)==0:
        if (year % 100)==0:
            if (year % 400)==0:
                return True
            else:
                return False
        else:
            return True     
    else:
        return False 
