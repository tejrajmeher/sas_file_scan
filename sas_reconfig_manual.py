
import sys
import os
import github
import threading
import time
import subprocess
import shutil
import datetime
import pandas as pd

def set_options():
    pd.set_option('display.width', 20000)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.max_rows', 5000)
    pd.set_option('display.precision',2)



class copycode:
    def __init__(self, src, tgt, ftype):
##        self.path={'srcDrive':r'O:\RRA\Portfolio\Mortgages',
##                   'tgtDrive':r'D:\Mortgage Strategy Team'}
        
        self.path={'srcDrive':src,
                   'tgtDrive':tgt}
        
        if ftype=='sas': self.ext=['.sas']
        if ftype=='egp': self.ext=['.egp']

        self.flist=[]
        self.err=[]
        
        self.maxthread=300
        self.activethread=0
        self.listFile(self.path['srcDrive'])
        self.createReplacePath()
        
        self.maxthread=30
        self.activethread=0
##        self.copyFiles()

        
    def listFile(self, path):
        self.activethread += 1
        thlist=[]
        try:
            for f in os.scandir(path):
                if f.is_file() and os.path.splitext(f.name)[1].lower() in self.ext:
                    self.flist.append(f)
                elif f.is_dir():
##                    if len(self.flist) > 10:
##                        break
                    if (len(self.flist) > 0) & (len(self.flist) % 500 == 0):
                        print(f"{len(self.flist)}, ", end='')
                        
                    th = threading.Thread(target=self.listFile,  args=(f.path,))
                    while self.activethread >= self.maxthread:
                        time.sleep(10)
                    thlist.append(th)
                    th.start()
                    
            self.activethread -= 1
            for th in thlist:
                th.join()
        except Exception as ex:
            self.err.append(['scan', path, ex])
        
    def createReplacePath(self):
        df=pd.DataFrame(self.flist, columns=['entry'])
        df['srcPath'] = df['entry'].apply(lambda x: x.path)
        df['tgtPath'] = df['srcPath'].apply(lambda x: x.replace(self.path['srcDrive'], self.path['tgtDrive']))
        self.df=df

    def copyFile(self):
        c = input(f"Number of files to copy = {len(self.df)}, proceed (y/n) ? ")
        if c.lower() != 'y':
            return

        thlist=[]
        self.copyCount=0
        self.d=300
        for ind, val in self.df.iterrows():
            if (self.copyCount) & (self.copyCount % self.d == 0):
                print(f"CopyCount={self.copyCount}, ErrorCount = {len(self.err)}\n", end='')
            th=threading.Thread(target=self._copyFile, args=(val,))
            while self.activethread >= self.maxthread:
                time.sleep(10)
            thlist.append(th)
            th.start()
            
##        for th in thlist:
##            th.join()
##        print(f"CopyCount={self.copyCount}, ErrorCount = {len(self.err)}\n", end='')
        
    def _copyFile(self, val):
        self.activethread += 1
        self.copyCount += 1
        
        pardir = os.path.dirname(val['tgtPath'])
        try:
            if not os.path.exists(pardir):
                os.makedirs(pardir)
            if not os.path.exists( val['tgtPath'] ):
                shutil.copy2(val['srcPath'],  val['tgtPath'] )
        except Exception as ex:
            self.err.append(['copy', val['srcPath'], ex])
            
        self.activethread -= 1


    
class rcrmcode:
    def __init__(self, org, ftype):
        self.git_org=org
        self.ftype=ftype.lower()
        
        self.git_url=’githuburl'
        self.path={'rcrmcode':r'networkpath',
                   'tempgit':f"{os.path.join(os.path.dirname(os.environ['appdata']),'local',self.git_org)}",
                   'drive':'d:\\'}
        self.path['tempgit_unix']=f"/{self.path['tempgit'].replace(os.sep, '/').replace(':/','/')}"

        self.reset_tempgit()
        self.get_gitbash()
        self.get_folders_local()

    def reset_tempgit(self):
##        if os.path.exists(self.path['tempgit']):
##            try:
##                os.system(f"rmdir /s /q {self.path['tempgit']}")
##            except:
##                pass
        if not os.path.exists(self.path['tempgit']):
            os.mkdir(self.path['tempgit'])
    
    def get_gitbash(self):
        path=os.popen('where git').read().strip()
        if not path:
            print(f"Could not find GIT installation in this system")
            sys.exit(0)
        parent=os.path.dirname( os.path.dirname(path) )
        if not os.path.exists(os.path.join(parent, "git-bash.exe")):
            print(f"Could not locate git-bash")
            sys.exit(0)
        self.path['git']=parent

    def get_folders_local(self):
        path=[p for p in os.scandir(self.path['rcrmcode'])]
        folder=[d.name for d in path if d.is_dir()]
        self.dir_local=folder

    def define_thvars(self):
        self.folder=[]
        self.maxthreads=100
        self.activethreads=0
        
    def get_reponame(self, dir_name):
        return ''.join(c for c in dir_name.replace(' ', '-')  if c.isalnum() or c=='-' or c=='_')

    def git_createrepo(self):
        gh=github.github()
        gh.login(github.cred.userid, github.cred.password)
        for d in self.dir_local:
            try:
                gh.create_repo(org_name=self.git_org, repo_name=self.get_reponame(d) )
            except Exception as ex:
                print(f"ERROR in creating repo {d}: {ex}")
                
    def git_emptyrepo(self):
        thlist=[]
        for d in self.dir_local:
            th=threading.Thread(target=self._git_emptyrepo, args=(d,))
            th.start()
            thlist.append(th)
        for th in thlist:
            th.join()
        
    def _git_emptyrepo(self, dir_name):
        repo_name = self.get_reponame(dir_name)
        cmd_list=[]
        cmd_list.append(f"cd '{self.path['tempgit_unix']}'")
        cmd_list.append(f"rm -rf {repo_name}")
        cmd_list.append(f"echo MSG: Cloning the existing repo")
        cmd_list.append(f"git clone git@{self.git_url}:{self.git_org}/{repo_name}.git")
        cmd_list.append(f"cd {repo_name}")
        cmd_list.append(f"echo MSG: Removing files from repo")
        cmd_list.append(f"git rm -rf *")
        cmd_list.append(f"git commit -m 'All files deleted @ {datetime.datetime.now()}'")
        cmd_list.append(f"echo MSG: Syncing with repmote repo")
        cmd_list.append(f"git push -u origin master")
        cmd_list.append(f"cd '{self.path['tempgit_unix']}'")
        cmd_list.append(f"rm -rf {repo_name}")
        
        gitshfile=f"{os.path.join(self.path['tempgit'], f'{repo_name}.sh')}"
        with open(gitshfile, 'w') as f:
            for l in cmd_list:
                _=f.write(f"{l}\n")
                
        popenlist=[os.path.join(self.path['git'],'bin','sh.exe'), gitshfile ]
        sprocess=subprocess.Popen(popenlist, shell=False)
        sprocess.wait()
        if sprocess.returncode==0:
            print(f"{repo_name} is successfully cleaned")
        else:
            print(f"{repo_name} is not cleaned")


    def git_clonerepo(self):
        thlist=[]
        for d in self.dir_local:
            th=threading.Thread(target=self._git_clonerepo, args=(d,))
            th.start()
            thlist.append(th)
        for th in thlist:
            th.join()
            
    def _git_clonerepo(self, dir_name):
        repo_name = self.get_reponame(dir_name)
        try:
            cmd_list=[]
            cmd_list.append(f"cd '{self.path['tempgit_unix']}'")
            cmd_list.append(f"rm -rf {repo_name}")
            cmd_list.append(f"git clone git@{self.git_url}:{self.git_org}/{repo_name}.git")
            cmd_list.append(f"cd '{self.path['tempgit_unix']}/{repo_name}'")
            cmd_list.append(f"git ls-files |wc -l > {repo_name}.txt")
            cmd_list.append(f"git ls-files >> {repo_name}.txt")

            gitshfile=f"{os.path.join(self.path['tempgit'], f'{repo_name}.sh')}"
            with open(gitshfile, 'w') as f:
                for l in cmd_list:
                    _=f.write(f"{l}\n")

            popenlist=[os.path.join(self.path['git'],'bin','sh.exe'), gitshfile ]
            sprocess=subprocess.Popen(popenlist, shell=False)
            sprocess.wait()            
        except Exception as ex:
            print(f"ERROR: Error in cloning repo '{repo_name}': (ex)")
            
        
    def git_filecount(self, reclone):
        thlist=[]
        self.git_fcount=[]
        for d in self.dir_local:
            th=threading.Thread(target=self._git_filecount, args=(d, reclone, self.git_fcount))
            th.start()
            thlist.append(th)
        for th in thlist:
            th.join()
        if self.git_fcount:
            self.git_fcount=pd.DataFrame(self.git_fcount)
            print(self.git_fcount)
            
    def _git_filecount(self, dir_name, reclone, fcount):
        if reclone:
            self._git_clonerepo(dir_name)
            
        repo_name = self.get_reponame(dir_name)
        try:
            txtfile=os.path.join(self.path['tempgit'],repo_name,f"{repo_name}.txt")
            nfile=0
            filecount=0
            _nfile=open(txtfile).readlines()
            if _nfile:
                nfile=int(_nfile[0].strip())
                if nfile>0:
                    fileext=[os.path.splitext(f)[1].strip() for f in _nfile[1:]]
                    filecount={ext:fileext.count(ext) for ext in fileext}
                    filecount['name']=repo_name
                    filecount['total']=nfile
                else:
                    filecount={'name':repo_name, 'total':nfile, '.sas':0, '.egp':0}
            fcount.append(filecount)
            
        except Exception as ex:
##            print(f"ERROR: Getting filecount in Repo={repo_name}: {ex}")
            filecount={'name':repo_name, 'total':nfile, 'error':ex}
            fcount.append(filecount)
                    
    def git_upload(self, copy):
        thlist=[]
        for d in self.dir_local:
            th=threading.Thread(target=self._git_upload, args=(d, copy))
            th.start()
            thlist.append(th)
        for th in thlist:
            th.join()
            
    def _git_upload(self, dir_name, copy):
        src=os.path.join(self.path['drive'], dir_name)
        repo_name = self.get_reponame(dir_name)
##        tgt=os.path.join(self.path['tempgit'], repo_name)
        src_unix = f"/{src.replace(os.sep, '/').replace(':/','/')}"

        try:
            if copy:
                t = os.path.join(self.path['tempgit'],repo_name)
##                shutil.rmtree(t)

##                cmd_list=[]
##                cmd_list.append(f"cd '{self.path['tempgit_unix']}'")
##                cmd_list.append(f"git clone git@{self.git_url}:{self.git_org}/{repo_name}.git")
##                gitshfile=f"{os.path.join(self.path['tempgit'], f'{repo_name}.sh')}"
##                with open(gitshfile, 'w') as f:
##                    for l in cmd_list:
##                        _=f.write(f"{l}\n")
##                popenlist=[os.path.join(self.path['git'],'bin','sh.exe'), gitshfile ]
##                sprocess=subprocess.Popen(popenlist, shell=False)
##                sprocess.wait()

                self.cp = copycode(src=src, tgt=t, ftype=self.ftype)
                self.cp.copyFile()
                
            cmd_list=[]
            cmd_list.append(f"cd '{self.path['tempgit_unix']}'")
##            if copy:
##                cmd_list.append(f"rm -rf {repo_name}")
##                cmd_list.append(f"git clone git@{self.git_url}:{self.git_org}/{repo_name}.git")
##                cmd_list.append(f"cd '{src_unix}'")
##                cmd_list.append(f"echo MSG: Copying files from {src}")
##                namestr=f"-name '*.{self.ftype}'"
##                cmd_list.append(rf"find . \( {namestr} \) -exec cp --parents {{}} '{self.path['tempgit_unix']}/{repo_name}' \;")

            cmd_list.append(f"cd '{self.path['tempgit_unix']}/{repo_name}'")
            cmd_list.append(f"echo MSG: Adding files to local repo")
            cmd_list.append(f"git add .")
            cmd_list.append(f"git status")
            cmd_list.append(f"git commit -m 'Files added on {datetime.datetime.now()}'")
            cmd_list.append(f"git ls-files |wc -l > {repo_name}.txt")
            cmd_list.append(f"echo MSG: Pushing to remote")
            cmd_list.append(f"git push -u origin master")
##            cmd_list.append(f"cd '{self.path['tempgit_unix']}'")
##            cmd_list.append(f"rm -rf {repo_name}")

            gitshfile=f"{os.path.join(self.path['tempgit'], f'{repo_name}.sh')}"
            with open(gitshfile, 'w') as f:
                for l in cmd_list:
                    _=f.write(f"{l}\n")

            popenlist=[os.path.join(self.path['git'],'bin','sh.exe'), gitshfile ]
            sprocess=subprocess.Popen(popenlist, shell=False)
            sprocess.wait()

            txtfile=os.path.join(self.path['tempgit'],repo_name,f"{repo_name}.txt")
            nfile=open(txtfile).readlines()
            if nfile:
                nfile=nfile[0].strip()
            else:
                nfile='NA'
            if sprocess.returncode==0:
                print(f"{repo_name} is successfully updated - Returncode={sprocess.returncode}, Number of files={nfile}")
            else:
                print(f"{repo_name} not updated")
        except Exception as ex:
            print(f"{repo_name} NOT updated: {ex}")


class manual_update(rcrmcode):
    def __init__(self, org, ftype):
        super().__init__(org, ftype) 
        self.dbc_columns=("""AccessCount,ArrayColElementType,ArrayColElementUdtName,ArrayColNumberOfDimensions,ArrayColScope,CharType,"""
                        """ColumnConstraint,ColumnFormat,ColumnId,ColumnLength,ColumnName,ColumnTitle,ColumnType,ColumnUDTName,CommentString,"""
                        """CompressValue,CompressValueList,Compressible,ConstraintCount,ConstraintId,CreateTimeStamp,CreatorName,DatabaseName,"""
                        """DecimalFractionalDigits,DecimalTotalDigits,DefaultValue,IdColType,LastAccessTimeStamp,LastAlterName,LastAlterTimeStamp,"""
                        """Nullable,SPParameterType,TTCheckType,TableName,TimeDimension,UpperCaseFlag,VTCheckType""").split(',')

    def list_files(self, repo_name):
        file=[]
        path = os.path.join(self.path['tempgit'], repo_name)
        self._list_file(path, file)
        return file
    
    def _list_file(self, path, file):
        thlist=[]
        for f in os.scandir(path):
            if f.is_file() and os.path.splitext(f.name)[1] in ['.sas','.egp'] :
                file.append(f)
            elif f.is_dir() and f.name!='.git':
                th=threading.Thread(target=self._list_file, args=(f.path, file) )
                thlist.append(th)
                th.start()
        for th in thlist:
            th.join()

    def modify_dbc_table(self, dir_name, overwrite=True):
        file = self.list_files(self.get_reponame(dir_name))
        summary=[]
        thlist=[]
        for f in file:
            th=threading.Thread(target=self._modify_dbc_table, args=(f.path, summary, overwrite))
            th.start()
            thlist.append(th)
        for th in thlist:
            th.join()
        self.summary=summary            ##to delete
        
        summary=pd.DataFrame(summary)
        summary.to_csv(os.path.join(self.path['tempgit'],f'{self.get_reponame(dir_name)}.csv'), index=False)
            
    def _modify_dbc_table(self, path, summary, overwrite):
        if os.path.splitext(path)[1]=='.sas':
            code=open(path, 'r').readlines()
            step='proc sql'
            found, start, end=False, 0, 0
            ischange=False
            try:
                for keyword in ['dbc.columnsv', 'dbc.columns']:
                    for n,l in enumerate(code):
                        if l.lower().replace(' ','').find(step.replace(' ','')) >= 0:
                            start=n
                        if l.lower().find(keyword) >= 0:
                            found=True
                        if found and l.lower().find('quit') >= 0:
                            end=n
                        if all([found, start, end]):
                            code_string=' '.join([l.strip() for l in code[start:end+1]])
                            col_start=code_string.lower().find('select', code_string.lower().find('select')+1) + len('select ')
                            col_end=code_string.lower().find('from', code_string.lower().find('from')+1)
                            col_torepl=code_string[col_start:col_end].strip()
##                            col_torepl=col_torepl.replace('(',' ( ').replace(')',' ) ')

                            process=True
                            for item in col_torepl.replace('(',' ( ').replace(')',' ) ').split(','):
                                if len(set(item.replace('.','. ').strip().split()).intersection(set(self.dbc_columns)))==0:
                                    process=False
                                
                            if process:
                                ischange=True
                                tbl_torepl=keyword.lower()
                                tbl_start=code_string.lower().find(tbl_torepl)
                                tbl_end=tbl_start + len(tbl_torepl)
                                tbl_torepl=code_string[tbl_start:tbl_end]
                                
                                col_withrepl='table_schema, table_name, column_name'
                                tbl_withrepl='&terasilo..information_schema.columns'.lower()
                                
                                code_string = code_string.replace(col_torepl, col_withrepl).replace(tbl_torepl, tbl_withrepl)
                                cols={'tablename':'table_name', 'columnname':'column_name', 'databasename':'table_schema'}
                                ws=''.join(f' {c} ' if c in ['(',')'] else c for c in code_string).split()
                                for n,w in enumerate(ws):
                                    if w.lower() in cols.keys():
                                        ws[n]=cols[w.lower()]
                                code_string=' '.join(ws)

                                summary.append({'path':path, 'type':'dbc_table', 'start':start, 'end':end,
                                                'prechange':' '.join([l.strip() for l in code[start:end+1]]),
                                                'post_change':code_string.replace(';', ';\n')})
                                for i in range(start,end+1):
                                    code[i]='\n'
                                code[start]=code_string.replace(';', ';\n')
                                
                            found, start, end=False, 0, 0
            except Exception as ex:
                print(f"{path}\t\t{ex}")

            if ischange and overwrite:
                open(path,'w').writelines(code)

            
if __name__=='__main__':
    set_options()
    sas=manual_update('TejrajMeher', ftype='sas')
    egp=manual_update('TejrajMeherEGP', ftype='egp')
    
