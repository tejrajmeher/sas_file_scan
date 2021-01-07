import zipfile
import os
import shutil
import xml.etree.ElementTree as et
import pandas as pd
import threading

def set_options():
    pd.set_option('display.width', 20000)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.max_rows', 5000)
    pd.set_option('display.precision',2)


class egp:
    def __init__(self):
        self.path=r'drivepath'
        self.inpfilename = "keywords.xlsx"

        self.error=[]
        self.result=[]
        self.indir='egpfiles'
        self.uncomp='uncompressed'
        self.inpfile = os.path.join(self.path, self.inpfilename)
        
        self.egpfiles=[f for f in os.scandir(os.path.join(self.path, self.indir))]
         
        self.get_kw()

        for file in self.egpfiles:
            self.scan_egp(file)
        self.df = pd.DataFrame(self.result)
        self.df.to_csv(r"outcsvpath")
        
    def get_kw(self):
        xls = pd.ExcelFile(self.inpfile)
        df = []
        for sh in xls.sheet_names:
            if sh != 'Full list of Type Codes':
                _ = pd.read_excel(xls, sh)
                _.insert(0,'sheet',sh)
                df.append(_)
        df = pd.concat(df, sort=False)
        
        self.kw_df = df.copy()
        self.kw = list(set(self.kw_df['CALC_VAL_TYCD'].to_list()))


    def scan_egp(self, file):
        epath = os.path.join(self.path, self.uncomp, os.path.splitext(file.name)[0] )
        xml_df = self.uncompress(file, epath)

        threadlist=[]
        for ind, row in xml_df.iterrows():
            th = threading.Thread(target=self.scan_file, args=(file, ind, row))
            threadlist.append(th)
            th.start()
            th.join()

        for th in threadlist:
            pass


    def scan_file(self, file, ind, row):
        try:
            with open(row['codepath'], 'r') as hndl:
                text = hndl.read().lower()
            text = ''.join([c if c.isalnum() or c=='_' or c.strip()=='' else ' ' for c in text])
            text = list(set(text.split()))

            d={'file':file.name, 'codename':row['egpcodepath']}
            for kw in self.kw:
                d[kw]="Yes" if kw.lower() in text else None
            self.result.append(d)
            
        except Exception as ex:
            self.error.append({'file':file.name, 'code':row['egpcodepath'], 'error':ex})
##            print(f"ERROR: {ind} {row['codepath']}")
        
    def uncompress(self, file, epath):
        with zipfile.ZipFile(file.path)as zr:
            zr.extractall(epath)

        return self.parse_xml(epath)


    def parse_xml(self, epath):
        xmlfile = [f for f in os.scandir(epath) if f.is_file()][0]
        print(epath, xmlfile.path)
        xml=et.parse(xmlfile, parser=et.XMLParser(encoding='utf-16'))
##        xml=et.fromstring(open(xmlfile).read(), parser=self.xmlp)
        xroot = xml.getroot()

        data = []
        elements = xml.findall('Elements')
        for el in elements:
            self.parse_xml_children(el, el.tag, data)
        
        df=pd.DataFrame(data)
        df['Type'].ffill(inplace=True)
        
        df['codename'] = df.apply(lambda x: x['text'] if (x['Type']=='SAS.EG.ProjectElements.CodeTask') & (x['tag']=='Elements-Element-Element-Label') else None, axis=1)
        df['codename'].ffill(inplace=True)
        code_subfolder = df[ (df['tag']=='Elements-Element-Element-ID') & (df['Type']=='SAS.EG.ProjectElements.CodeTask') ][['text','codename']].rename(columns={'text':'subfolder'})
        code_container = df[ (df['tag']=='Elements-Element-Element-Container') & (df['Type']=='SAS.EG.ProjectElements.CodeTask') ][['text','codename']].rename(columns={'text':'containerid'})

        df['containername'] = df.apply(lambda x: x['text'] if (x['Type']=='SAS.EG.ProjectElements.PFD') & (x['tag']=='Elements-Element-Element-Label') else None, axis=1)
        df['containername'].ffill(inplace=True)
        container = df[ (df['tag']=='Elements-Element-Element-ID') & (df['Type']=='SAS.EG.ProjectElements.PFD') ][['text','containername']].rename(columns={'text':'containerid'})

        df = pd.merge(code_subfolder, code_container, how='left', on='codename')
        df = pd.merge(df, container, how='left', on='containerid')
        df.sort_values(['containername','codename'], inplace=True)

        df['codepath'] = df['subfolder'].apply(lambda x:os.path.join(epath, x, 'code.sas' ))
        df['egpcodepath'] = df.apply(lambda x: f"{x['containername']}/{x['codename']}.sas", axis=1)
        
        return df.reset_index(drop=True)
    

    def parse_xml_children(self, el, val, data):
        exclude = ['Elements-Element-TextElement-Text']
        if el:
            for _el in el:
                for k,v in _el.attrib.items():
                    if f"{val}-{_el.tag}".strip() not in exclude:
                        data.append({'tag':f"{val}-{_el.tag}", k:v})
                self.parse_xml_children(_el, f"{val}-{_el.tag}", data)
        else:
            if f"{val}".strip() not in exclude:
                data.append({'tag':f"{val}", 'text':f"{el.text}"})

    
    
                
        
    
if __name__=='__main__':
    set_options()
    self=egp()
