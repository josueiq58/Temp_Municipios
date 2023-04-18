import os
import gzip
import json
import pandas as pd
import requests as rq
from datetime import datetime, timedelta
import logger
import numpy as np

class DataProcessor:
    '''
    Class to implement the pipeline of the exercise
    '''
    def __init__(self):
        self.l = logger.Logger(__name__)
        self.dt_format = "%Y%m%dT%H"
        
  
    def download_api_data(self,temp):
        '''
        Get the zip file containing the data provided by the API
        '''
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
        
        if temp == 'daily':
            url = f'https://smn.conagua.gob.mx/webservices/?method=1'
        elif temp == 'hourly':
            url = f'https://smn.conagua.gob.mx/webservices/?method=3'
            
        self.l.log_info('Downloading data from API')
        intento=0
        while intento<=5:
            response = rq.get(url, headers=headers)
            if response.status_code!=200:
                self.l.log_info(f'Response from the API: {response.status_code}')
                intento+=1
            else:
                self.l.log_info(f'Response from the API: {response.status_code}')
                with open(f'{temp}.gz', 'wb') as f:
                    f.write(response.content)
                self.l.log_info(f'Data succesfully written to {temp}.gz')
                break
    
    
    def load_api_data(self,zip_name):
        '''
        Uncompress the API data to read it in tabular form
        '''
        self.l.log_info('Uncompressing data')
        with gzip.open(f'{zip_name}.gz', 'rb') as f:
            uncompressed_data = f.read()
        string = uncompressed_data.decode("utf-8")
        self.l.log_info('Data uncompressed successfully')
        data = pd.DataFrame(json.loads(string))
        #data["ides"] = pd.to_numeric(data["ides"])
        #data["idmun"] = pd.to_numeric(data["idmun"])
        return data

    
    def update_historic_data(self,df,temp):
        file = f'historico\\historic_data_{temp}.csv'
        if file.split('\\')[-1] not in os.listdir('historico'):
            df.to_csv(file,index=False)
        else:
            historic = pd.read_csv(file)
            new_historic = pd.concat([historic,df]).drop_duplicates().reset_index(drop=True)
            new_historic.to_csv(file,index=False)
    
    def current_record(self,temp):
        file = f'historico\\last_record_{temp}.csv'
        if file.split('\\')[-1] not in os.listdir('historico'):
            pass
        else:
            current=pd.read_csv(file)
            return current
        
    def update_last_record(self,df,temp):
        if temp == 'daily':
            df[df['dloc'] == df['dloc'].max()].to_csv(f'historico\\last_record_{temp}.csv',index = False)
        elif temp == 'hourly':
            df[df['hloc'] == df['hloc'].max()].to_csv(f'historico\\last_record_{temp}.csv',index = False)
        
    
    def update_avg(self, df):
        '''
        Get the average of the last available 2 days by municipality 
        '''
        #nan_value = float("NaN")
        file0 = 'historico\\last_record_hourly.csv'
        pivote = pd.read_csv(file0)
        avg=pd.concat([pivote,df])
        self.l.log_info('Getting the average temperature and precipitation of last 2 hours')
        avg = pd.pivot_table(avg, values=['prec', 'temp'], index=['ides', 'idmun'], aggfunc=np.mean).reset_index() 
 
        file = 'historico\\average_2_hours.csv'
        if file.split('\\')[-1] not in os.listdir('historico'):
            avg.to_csv(file,index=False)
        else:
            old_avg = pd.read_csv(file)
            new_avg = pd.concat([old_avg,avg]).drop_duplicates().reset_index(drop=True)
            new_avg.to_csv(file,index=False)
        return avg
    
    
    def read_most_recent(self,directory):
        '''
        Get the file with most recent date
        '''
        file_list = os.listdir(directory)
        self.l.log_info('Getting file names of data_municipios directory')
        s_list = sorted([fname for fname in file_list if fname.endswith('.csv')])
        recent_file = s_list[-1]
        self.l.log_info('Reading most current file')
        data = pd.read_csv(f'{directory}\\{recent_file}')
        self.l.log_info('The file was read succesfully')
        return data
    
    
    def join_data(self,municipios, api):
        '''
        Integrate the API data to the existing file
        '''
        self.l.log_info('Joining API data with existing data')
        joined = municipios.merge(api, how='left', left_on=['Cve_Ent', 'Cve_Mun'], right_on=['ides', 'idmun'])
        joined= joined[['Cve_Ent','Cve_Mun','Value','prec','temp']]
        return joined
    
    
    def write_results(self, data):
        '''
        Save the joined data along with a copy
        '''
        fecha = datetime.now().strftime(format=self.dt_format)
        path = os.getcwd() + '\\current\\'
        file_name = f'results_{fecha}.csv'
        data.to_csv(path + file_name, index=False)
        return 1
