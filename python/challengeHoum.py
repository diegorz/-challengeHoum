#############################################################################
#                      CHALLENGE HOUM                                       #
# DESCRIPCION   : Este script responde las preguntas del challenge.         #
# AUTOR         : diegorz@me.com                                            #
# CAMBIOS       :                                                           #
# VER.  FECHA            AUTOR              COMENTARIOS                     #                 
# --------------------------------------------------------------------------#
# 1.0   2022/08/08      diegorz             Version Inicial                 #
# --------------------------------------------------------------------------#
#############################################################################
import json
import time
from datetime import timedelta
import requests
import pandas as pd
import sys

time_init = time.time()

###############################################################################
################## Get values from configuration file #########################
###############################################################################
conf = json.loads(open('config.json').read())
# API config
api_key=conf['api_key']
base_url="https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"

#UnitGroup sets the units of the output - us or metric
unit_group='metric'

#include sections
#values include days,hours,current,alerts
include_time="days"

#Optional start and end dates
#If nothing is specified, the forecast is retrieved. 
#If start date only is specified, a single historical or forecast day will be retrieved
#If both start and and end date are specified, a date range will be retrieved
start_date = ''
end_date = ''



##########################################
########### API Query function ###########
##########################################
def api_query_results(ApiQuery, ApiKey, UnitGroup, Include, StartDate, EndDate):
    #append the start and end date if present
    if (len(StartDate)):
        ApiQuery+="/"+StartDate
        if (len(EndDate)):
            ApiQuery+="/"+EndDate
    #Url is completed. Now add query parameters (could be passed as GET or POST)
    ApiQuery+="?key="+ApiKey
    
    #append each parameter as necessary
    if (len(UnitGroup)):
        ApiQuery+="&unitGroup="+UnitGroup

    if (len(Include)):
        ApiQuery+="&include="+Include
    
    #print('\nRunning query URL: ', ApiQuery)
    try: 
        response = requests.get(ApiQuery)
        body = response.json()
        return body
    except Exception as e:
        print('Error: ',e)
        sys.exit()



#################################
########## CHALLENGE ############
#################################

# Datasets to DataFrames
df_props = pd.read_csv('../datasets/properties.csv')
df_users = pd.read_csv('../datasets/users.csv')
df_visits = pd.read_csv('../datasets/visits.csv')


############################################
# ¿Cuántas visitas se realizaron en total? #
############################################
print('\n¿Cuántas visitas se realizaron en total?')

done_visits = df_visits[df_visits.status=='Done'].shape[0]
print('Respuesta:', done_visits, 'visitas en total')


########################################################
# ¿Cuál es el promedio de propiedades por propietario? #
########################################################
print('\n¿Cuál es el promedio de propiedades por propietario?')

props_mean = df_users.groupby('user_id')['property_id'].count().mean().round(2)
print('Respueta:', props_mean, 'propiedades en promedio')


###################################################################################################################
# ¿Cuál era la temperatura promedio de todas las visitas que realizó en la propiedad del propietario con ID 2?    #
#                                                                                                                 #
# Supuesto: Asumo que se necesita saber la temperatura promedio de los días en que se realizaron visitas          #
#           a las propiedades del ID 2                                                                            #
###################################################################################################################
print('\n¿Cuál era la temperatura promedio de todas las visitas que se realizó en la propiedad del propietario con ID 2?')

property_id_visit = df_users[df_users.user_id==2].property_id
visits_done_id = df_visits[(df_visits.property_id.isin(property_id_visit.unique())) & (df_visits.status=='Done')]

lat_lon_props_id = list(zip(df_props[df_props.property_id.isin(property_id_visit.unique())]['latitude']
                        ,df_props[df_props.property_id.isin(property_id_visit.unique())]['longitude']
                        ,visits_done_id.begin_date.str.split('T').str.get(0)
                        ,visits_done_id.end_date.str.split('T').str.get(0)))

df_id2_total = pd.DataFrame()
for lat, lon, start_date, end_date in lat_lon_props_id:
    try:
        api_query_latlon=base_url
        #append the latitude and longitude
        api_query_latlon+=str(lat)+","+str(lon)
        body_rain = api_query_results(ApiQuery=api_query_latlon, ApiKey=api_key, UnitGroup=unit_group, Include=include_time
                                    ,StartDate=start_date, EndDate=end_date)

        df_id2 = pd.DataFrame.from_dict(body_rain['days'])
        df_id2_total = pd.concat([df_id2_total,df_id2], ignore_index=True)
        
    except Exception as e:
        print('Error: ',e)
        sys.exit()

temp_mean_id2 = df_id2_total.temp.mean().round(2)
print('Respuesta:', temp_mean_id2,'ºC')

#############################################################################
# ¿Cuál es la temperatura promedio de las visitas para los días con lluvia? #
#                                                                           #
# Supuesto: Asumo que es necesario conocer la temperatura promedio          #
#           de los días con lluvia y con visitas realizadas                 #
#############################################################################
print('\n¿Cuál es la temperatura promedio de las visitas para los días con lluvia?')

visits_done_rain = df_visits[df_visits.status=='Done']
latlon_visits_rain = list(zip(df_props[df_props.property_id.isin(visits_done_rain.property_id.unique())]['latitude']
                            ,df_props[df_props.property_id.isin(visits_done_rain.property_id.unique())]['longitude']
                            ,visits_done_rain.begin_date.str.split('T').str.get(0)
                            ,visits_done_rain.end_date.str.split('T').str.get(0)))

df_rain_total = pd.DataFrame()
for lat, lon, start_date, end_date in latlon_visits_rain:
    try:
        api_query_latlon=base_url
        #append the latitude and longitude
        api_query_latlon+=str(lat)+","+str(lon)
        body_rain = api_query_results(ApiQuery=api_query_latlon, ApiKey=api_key, UnitGroup=unit_group, Include=include_time
                                    ,StartDate=start_date, EndDate=end_date)

        df_rain = pd.DataFrame.from_dict(body_rain['days'])
        if 'rain' in df_rain['preciptype'][0]: 
            df_rain_total = pd.concat([df_rain_total,df_rain], ignore_index=True)
        else: continue
        df_rain_total_compare = pd.concat([df_rain_total,df_rain], ignore_index=True)
    except Exception as e:
        print('Error: ',e)
        sys.exit()


temp_mean_rain = df_rain_total.temp.mean().round(2)
print('Respuesta:', temp_mean_rain,'ºC')


#########################################################################################
# ¿Cuál es la temperatura promedio para las visitas realizadas en la localidad de Suba? #
#########################################################################################
print('\n¿Cuál es la temperatura promedio para las visitas realizadas en la localidad de Suba?')

props_id_suba = df_props[df_props.localidad=='Suba'].property_id
visits_suba = df_visits[(df_visits.property_id.isin(props_id_suba)) & (df_visits.status=='Done')]

latlon_props_suba = list(zip(df_props[df_props.property_id.isin(props_id_suba.unique())]['latitude']
                            ,df_props[df_props.property_id.isin(props_id_suba.unique())]['longitude']
                            ,visits_suba.begin_date.str.split('T').str.get(0)
                            ,visits_suba.end_date.str.split('T').str.get(0)))

df_suba_total = pd.DataFrame()
for lat, lon, start_date, end_date in latlon_props_suba:
    try:
        api_query_latlon=base_url
        #append the latitude and longitude
        api_query_latlon+=str(lat)+","+str(lon)
        body_suba = api_query_results(ApiQuery=api_query_latlon, ApiKey=api_key, UnitGroup=unit_group, Include=include_time, StartDate=start_date, EndDate=end_date)
        
        df_suba = pd.DataFrame.from_dict(body_suba['days'])
        df_suba_total = pd.concat([df_suba_total,df_suba], ignore_index=True)
    except Exception as e:
        print('Error: ',e)
        sys.exit()

temp_mean_suba = df_suba_total.temp.mean().round(2)
print('Respuesta:', temp_mean_suba,'ºC')


print('\n\nTiempo ejecución script: ', str(timedelta(seconds=(time.time()-time_init))))