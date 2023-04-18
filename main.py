import pandas as pd
import sys
import data_processor
import logger

log = logger.Logger(__name__)
dp = data_processor.DataProcessor()

def main():
    #decarga de la información
    dp.download_api_data('daily')
    dp.download_api_data('hourly')
    
    #Carga de los datos
    api_daily_data = dp.load_api_data('daily')
    api_hourly_data = dp.load_api_data('hourly')

    #Actualización de histórico
    dp.update_historic_data(api_daily_data, 'daily')
    dp.update_historic_data(api_hourly_data, 'hourly')
    
    #Respaldo de la ultima versión de sitio
    data=dp.current_record('hourly')

    #Actualización de los archivos de la ejecución
    dp.update_last_record(api_daily_data,'daily')
    dp.update_last_record(api_hourly_data,'hourly')
    
    #Carga de útimo archivo municipio
    Current_municipios = dp.read_most_recent("data_municipios")
    
    #Promedio de las últimas dos horas
    avg = dp.update_avg(data)
    
    #Cruce final para generar salida y escribor resultados
    joined_data = dp.join_data(Current_municipios,avg) #PENDIENTE DEFINIR EL DF FINAL
    dp.write_results(joined_data)
    
    
    
if __name__ == '__main__':
    try:
        log.log_info('Starting the process')
        main()
        log.log_info('Process finished')
        sys.exit(0)
    except Exception as e:
        log.log_exception('There was an error ')
        sys.exit(1)