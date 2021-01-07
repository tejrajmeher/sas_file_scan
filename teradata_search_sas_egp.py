
import sys
import os
import threading 
import pandas as pd
import time
import zipfile
import shutil
import math
import datetime

def searchtdinsas(tdsearch, rp, f, fname, ctime1, mtime1):
    global filelist, errlist
    global filethreads
    global copysasfile, tmppath

    tdconn = "TD NOT checked"
    
    
    if (tdsearch=="yes"):
        filethreads+=1
        try:
            if (copysasfile=="yes"):
                t=tmppath + "\\" + threading.currentThread().getName() + ".sas"
                shutil.copy(f, t)
                myfile=open(t)
            else:
                myfile=open(f)

            filethreads-=1
                
            if 'myfile' in vars():
                fs=myfile.read().lower()
                myfile.close()
                del myfile

                deletesasfile(t)
                
                for ts in tdconnstrings:
                    if ts in fs:
                        tdconn = "TD found-" + ts
                        filelist.append([rp, f, fname, tdconn, ctime1, mtime1])
                        deletesasfile(t)
                        return
                    
                tdconn = "TD NOT found"
            
        except Exception as ex:
            filethreads-=1
            if (tdconn == "TD NOT checked"):
                tdconn=str(ex)
            deletesasfile(t)
    
    filelist.append([rp, f, fname, tdconn, ctime1, mtime1])
    deletesasfile(t)
    return
    

def deletesasfile(t):
    try:
        os.remove(t)
    except:
        pass

def searchtdinegp(tdsearch, rp, f, fname, ctime1, mtime1):
    global filelist, errlist
    global filethreads
    global tmppath
    
    tdconn ="TD NOT checked"
    
    if (tdsearch=="yes"):
        filethreads+=1
        try:
            tmpfolder=tmppath + "\\" + threading.currentThread().getName()
            
            t=tmppath + "\\" + threading.currentThread().getName() + ".egp"
            shutil.copy(f, t)
            
            if os.path.exists(tmpfolder):
                shutil.rmtree(tmpfolder, ignore_errors=True)
            os.makedirs(tmpfolder)
            with zipfile.ZipFile(t) as zipref:
                zipref.extractall(tmpfolder)

            deletesasfile(t)

            filethreads-=1
            for root, dirs, files in os.walk(tmpfolder):
                for file in files:
                    if os.path.splitext(file.lower())[1] in extlist:
                        myfile=open(root + "\\" + file)
                        fs=myfile.read().lower()
                        myfile.close()
                        del myfile
                        
                        for ts in tdconnstrings:
                            if ts in fs:
                                tdconn = "TD found-"+ts
                                filelist.append([rp, f, fname, tdconn, ctime1, mtime1])
                                deletesasfile(t)
                                shutil.rmtree(tmpfolder, ignore_errors=True)
                                return
                            
                    deletesasfile(root + "\\" + file)
                        
                        
            tdconn = "TD NOT found"
            deletesasfile(t)
            shutil.rmtree(tmpfolder, ignore_errors=True)
            
        except Exception as ex:
            filethreads-=1
            if (tdconn=="TD NOT checked"):
                tdconn=str(ex)
            
            deletesasfile(t)
            
        
        
    filelist.append([rp, f, fname, tdconn, ctime1, mtime1])
    deletesasfile(t)
    return



def tej(rp, tdsearch, lvl):
    global filelist, errlist
    global maxfolderthreads, maxfilethreads, filethreads, folderthreads, folderthreadsdead

    
    threadlist=[]
    folderthreadlist=[]

##    printmsg(lvl, "Start")
    st=time.time()
    try:
        if (filethreads < maxfilethreads):
            flist=sorted(os.scandir(rp), key=lambda x:(x.is_dir(), x.name), reverse=False)
        else:
            flist=sorted(os.scandir(rp), key=lambda x:(x.is_dir(), x.name), reverse=True)
        for f in flist:
            try:
                if f.is_dir():
                    th = threading.Thread(target=tej, args=(f.path.strip(),tdsearch, lvl+1))
                    if (folderthreads > maxfolderthreads  and  lvl <= 5):
                        time.sleep(60)
                    th.start()
                    folderthreads+=1
                    threadlist.append(th)                      
                else:
                    ext = os.path.splitext(f.name.lower())[1]
                    if ext in extlist:
                        while (filethreads >= maxfilethreads and threading.active_count() > 100):
                            print(f"file thread waiting lvl={lvl} filethreads={filethreads} maxfilethreads={maxfilethreads} filelistcount={len(filelist)} \n")
                            time.sleep(60)
                        ctime=os.path.getctime(f.path)
                        ctime1=time.strftime("%Y-%m-%d", time.localtime(ctime) )
                        mtime=os.path.getmtime(f.path)
                        mtime1=time.strftime("%Y-%m-%d", time.localtime(mtime) )
                        
                        if (ext == ".sas"):
                            th = threading.Thread(target=searchtdinsas, args=(tdsearch, rp, f.path.lower(), f.name, ctime1, mtime1,) )
                            th.daemon=True
                            th.start()
                            threadlist.append(th)
                        elif (ext == ".egp"):
                            th = threading.Thread(target=searchtdinegp, args=(tdsearch, rp, f.path.lower(), f.name, ctime1, mtime1,) )
                            th.daemon=True
                            th.start()
                            threadlist.append(th)
                        else:
                            filelist.append([rp, f.path, f.name, "TD NOT checked", ctime1, mtime1])
##                    printmsg(lvl, "Other files__")
                    
            except Exception as ex:
                errlist.append([rp, ex])
            
    except Exception as ex:
        errlist.append([rp, ex])
    
    folderthreads-=1
    
    folderthreadsdead+=1
    for th in threadlist:
        th.join()
    folderthreadsdead-=1
        
##    printmsg(lvl, "End")


def printmsg():    ## lvl, flag=""):
    global maxfolderthreads, maxfilethreads, filethreads, folderthreads, folderthreadsdead
    global cpu_percent
    global print_stop

    print("Printing log...\n")
    while (print_stop==0):
        time.sleep(5)
        print(f"{datetime.datetime.now()} activeFolderthreads={folderthreads} deadFolderthreads={folderthreadsdead} filethreads={filethreads} filelistcount={len(filelist)} errlist={len(errlist)} ")
        
    print("Ending log...")


def exp2csv():
    global export, print_stop
    
    if export != "yes":
        return
    
    ##    convert to dataframes and export
    while (print_stop==0):
        try:
            _exp2csv()
        except:
            pass
        finally:
            time.sleep(600)


def _exp2csv():
    global filelist, errlist
    global csvfile, summcsvfile, errcsvfile
    global filethreads
    global export, print_stop
    global predf, preerrdf
    global df, err, summ, summ1
    
    if (len(filelist) > 0):
        print("Exporting...")
        df = pd.DataFrame(filelist, columns=['root','path','filename','tdconn','ctime','mtime'])
        df.drop('path', axis=1, inplace=True)
        df.insert(0,'drive', df['root'].apply(lambda x:x[:1].upper()))
        df.insert(1,'ext', df['filename'].apply(lambda x:x[-3:].lower()))
        df['cyear']=df['ctime'].str.split('-').str[0]
        df['myear']=df['mtime'].str.split('-').str[0]
        df.insert(0, 'sno', df.index)
        df.to_csv(csvfile, sep=",", index=False)

        
##        summ = pd.pivot_table(df, index='cyear', columns='myear', aggfunc='count', values='sno').fillna(0)
        summ1=df.groupby(['ext','drive','tdconn'])['sno'].size().reset_index().sort_values(['tdconn','ext'])
        summ1.to_csv(summcsvfile, sep=",", index=False)
        
            
        err = pd.DataFrame(errlist, columns=['root','errormsg'])
        err['error']=err['errormsg'].apply(lambda x: 'Access is denied' if 'Access is denied' in str(x) else 'system cannot find the path' if 'system cannot find the path' in str(x) else 'unknown')
        err.insert(0,'drive', err['root'].apply(lambda x:x[:1].upper()))
        err.insert(0,'sno', err.index)
        err.to_csv(errcsvfile, sep=",", index=False)

        print(f"Exporting complete. Records exported = {len(df)}")

        
if __name__ == "__main__":    
    pd.set_option('display.width', 20000)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.max_rows', 5000)
    pd.set_option('precision',2)
    
    global extlist, tdconnstrings
    global filelist, errlist
    global threadlistmain
    global maxfolderthreads, maxfilethreads, filethreads, folderthreads, folderthreadsdead
    global tmppath

    print_stop=0
    filelist=[]
    errlist=[]
    
    threadlistmain=[]
    filethreads = 0
    folderthreads=0
    folderthreadsdead=0

    
    
    ###     USER INPUT   #####
    ###########################################################################
    extlist = [".sas",".emp",".egp"]
    tdconnstrings=["&terasilo", "connect to teradata", "teradata", "&teradbase", "&terauser"]
    maxfolderthreads=1000
    maxfilethreads=400
    
    tdsearch="yes"
    copysasfile="yes"
    
    path=["M:\\"]
    
    export="yes"
    csvfile=r'csvpath'
    summcsvfile=r'csvpath'
    errcsvfile=r'csvpath'
    ###########################################################################

    thpr=threading.Thread(target=printmsg, args=() )
    thpr.start()
    
    starttime=time.time()
    print(f"Starting @ {datetime.datetime.now()}")

    print(f"\nPATH = {path}\n")
    
    
    ##delete the temp folders at the start if exists and recreate it
    print("Creating temp directory...\n")
    tmppath=os.getenv('temp') + "\\tejinv" + str(math.floor(time.time()))
    shutil.rmtree(tmppath, ignore_errors=True)
    os.makedirs(tmppath)
    
    
    for ph in path:
        thm = threading.Thread(target=tej,  args=( str(ph).strip(), tdsearch, 1)   )
        thm.daemon=True
        thm.start()
        threadlistmain.append(thm)

    thexp=threading.Thread(target=exp2csv, args=() )
    thexp.start()
    
    for thme in threadlistmain:
        thme.join()

    ####  stop printing log  
    print_stop=1
    thpr.join()
    thexp.join()
    
##    delete the temp folders used in egp tdsearch
    print("\nAll threads complete. Removing temp folder...")
    shutil.rmtree(tmppath, ignore_errors=True)
    
    
##    convert to dataframes and export
    df = pd.DataFrame(filelist, columns=['root','path','filename','tdconn','ctime','mtime'])
    df.drop('path', axis=1, inplace=True)
    df.insert(0, 'sno', df.index)
    df.insert(1,'drive', df['root'].apply(lambda x:x[:1].upper()))
    df.insert(2,'ext', df['filename'].apply(lambda x:x[-3:].lower()))
    df['cyear']=df['ctime'].str.split('-').str[0]
    df['myear']=df['mtime'].str.split('-').str[0]
    if export=="yes":
        df.to_csv(csvfile, sep=",", index=False)

    
    summ = pd.pivot_table(df, index='cyear', columns='myear', aggfunc='count', values='sno').fillna(0)
    summ1=df.groupby(['ext','drive','tdconn'])['sno'].size().reset_index().sort_values(['tdconn','ext'])
    if export=="yes":
        summ1.to_csv(summcsvfile, sep=",", index=False)
    
        
    err = pd.DataFrame(errlist, columns=['root','errormsg'])
    err.insert(0,'sno', err.index)
    err['error']=err['errormsg'].apply(lambda x: 'Access is denied' if 'Access is denied' in str(x) else 'system cannot find the path' if 'system cannot find the path' in str(x) else 'unknown')
    err.insert(1,'drive', err['root'].apply(lambda x:x[:1].upper()))
    if export=="yes":
        err.to_csv(errcsvfile, sep=",", index=False)


##    print runtime
    endtime=time.time()
    print(f"\n\nTIME TAKEN :  {endtime-starttime}  {round((endtime-starttime)/60)} minutes")
    

#    df.groupby(['ext','tdconn'])['sno'].count() inventory


##split by semicolon to get individual sentence
##segregate all steps - data and proc
##one class for data and one for proc
##    each proc will be a class
##analyse sentence for keywords - count and pattern





