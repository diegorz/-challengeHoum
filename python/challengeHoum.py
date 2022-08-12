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
from io import StringIO 

time_init = time.time()
print('\n[INFO] Comienzo de ejecución\n')

###############################################################################
################## Get values from configuration file #########################
###############################################################################
conf = json.loads(open('config.json').read())
# API config
# API Key
api_key=conf['api_key']
# URL Base of API
base_url="https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
# UnitGroup 
unit_group='metric'
# Include sections
include_time='days'
# Optional columns for reduce query cost
elements=''
# Optional type of API response
file_type='csv'
# Optional start and end dates
start_date = ''
end_date = ''



##########################################
########### API Query function ###########
##########################################
def api_query_results(ApiQuery, ApiKey, UnitGroup, Include, Elements, ContentType, StartDate, EndDate):
    """
    Query to API and return a json with response
    
    Arguments:
    ApiQuery: String with URL Base + latitude and longitude coordinates.
    ApiKey: User API Key.
    UnitGroup: Units of the output - us or metric.
    Include: Include days,hours,current,alerts values.
    Elements: Columns that must extract.
    ContentType: type (JSON or CSV) of API response.
    StartDate: If nothing is specified, the forecast is retrieved. If start date only is specified, a single historical or forecast day will be retrieved.
    EndDate: If nothing is specified, the forecast is retrieved. If both start and and end date are specified, a date range will be retrieved.
    """
    # Append the start and end date if present
    if (len(StartDate)):
        ApiQuery+="/"+StartDate
        if (len(EndDate)):
            ApiQuery+="/"+EndDate
    # Url is completed. Now add query parameters (could be passed as GET or POST)
    ApiQuery+="?key="+ApiKey
    
    # Append each parameter as necessary
    if (len(UnitGroup)):
        ApiQuery+="&unitGroup="+UnitGroup

    if (len(Include)):
        ApiQuery+="&include="+Include
    
    if (len(Elements)):
        ApiQuery+="&elements="+Elements
    
    if (len(ContentType)):
        ApiQuery+="&contentType="+ContentType

    try: 
        response = requests.get(ApiQuery)
        return response.content
    except Exception as e:
        print('[Error]', e)
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

try:
    # He tomado todas las visitas realizadas (Done) y he sacado la dimensionalidad de filas para obtener el total
    done_visits = df_visits[df_visits.status=='Done'].shape[0]
    print('Respuesta:', done_visits, 'visitas en total')
except Exception as e:
        print('[Error]', e)
        sys.exit()


########################################################
# ¿Cuál es el promedio de propiedades por propietario? #
########################################################
print('\n¿Cuál es el promedio de propiedades por propietario?')

try:
    # He agrupado las propiedades por propietario y luego he calculado el promedio sobre esas cantidades (propiedades)
    props_mean = df_users.groupby('user_id')['property_id'].count().mean().round(2)
    print('Respueta:', props_mean, 'propiedades en promedio')
except Exception as e:
        print('[Error]', e)
        sys.exit()

###################################################################################################################
# ¿Cuál era la temperatura promedio de todas las visitas que realizó en la propiedad del propietario con ID 2?    #
#                                                                                                                 #
# Supuesto: Asumo que se necesita saber la temperatura promedio de los días en que se realizaron visitas          #
#           a las propiedades del ID 2                                                                            #
###################################################################################################################
print('\n¿Cuál era la temperatura promedio de todas las visitas que se realizó en la propiedad del propietario con ID 2?')

# Selecciono las propiedades que corresponden al dueño con id 2 y luego me quedo con las visitas realizadas a esas propiedades
property_id_visit = df_users[df_users.user_id==2].property_id
visits_done_id = df_visits[(df_visits.property_id.isin(property_id_visit.unique())) & (df_visits.status=='Done')]

# Se arma una lista con cada conjunto de latitud, longitud, fecha inicio visita y fecha final visita por cada 
# visita realizada a esas propiedades(de ID 2) en particular
lat_lon_props_id = list(zip(df_props[df_props.property_id.isin(property_id_visit.unique())]['latitude']
                        ,df_props[df_props.property_id.isin(property_id_visit.unique())]['longitude']
                        ,visits_done_id.begin_date.str.split('T').str.get(0)
                        ,visits_done_id.end_date.str.split('T').str.get(0)))

df_id2_total = pd.DataFrame()
# Por cada conjunto de datos de la lista anterior se hace una consulta a la API
for lat, lon, start_date, end_date in lat_lon_props_id:
    try:
        api_query_latlon=base_url
        # Append the latitude and longitude
        api_query_latlon+=str(lat)+","+str(lon)
        # Set columns necessary
        columns_select='datetime,temp'
        body_id = api_query_results(ApiQuery=api_query_latlon, ApiKey=api_key, UnitGroup=unit_group, Include=include_time
                                    ,Elements=columns_select, ContentType=file_type, StartDate=start_date, EndDate=end_date)
    
        # Se transforma el objeto csv retornado a dataframe de pandas
        df_id2 = pd.read_csv(StringIO(body_id.decode('utf-8')))
        # Se concatena cada dataframe generado por cada consulta a la API a un dataframe que consolida todos los resultados
        df_id2_total = pd.concat([df_id2_total,df_id2], ignore_index=True)
        
    except Exception as e:
        print('[Error]', e)
        sys.exit()

# Se calcula el promedio de las temperaturas obtenidas para aquellos días y locaciones consultados a la API
temp_mean_id2 = df_id2_total.temp.mean().round(2)
print('Respuesta:', temp_mean_id2,'ºC')


#############################################################################
# ¿Cuál es la temperatura promedio de las visitas para los días con lluvia? #
#                                                                           #
# Supuesto: Asumo que es necesario conocer la temperatura promedio          #
#           de los días con lluvia y con visitas realizadas                 #
#############################################################################
print('\n¿Cuál es la temperatura promedio de las visitas para los días con lluvia?')

# Selecciono todas las visitas realizadas
visits_done_rain = df_visits[df_visits.status=='Done']

# Se arma una lista con cada conjunto de latitud, longitud, fecha inicio visita y fecha final visita por cada visita realizada
latlon_visits_rain = list(zip(df_props[df_props.property_id.isin(visits_done_rain.property_id.unique())]['latitude']
                            ,df_props[df_props.property_id.isin(visits_done_rain.property_id.unique())]['longitude']
                            ,visits_done_rain.begin_date.str.split('T').str.get(0)
                            ,visits_done_rain.end_date.str.split('T').str.get(0)))

df_rain_total = pd.DataFrame()
# Por cada conjunto de datos de la lista anterior se hace una consulta a la API
for lat, lon, start_date, end_date in latlon_visits_rain:
    try:
        api_query_latlon=base_url
        # Append the latitude and longitude
        api_query_latlon+=str(lat)+","+str(lon)
        # Set columns necessary
        columns_select='datetime,temp,preciptype'
        body_rain = api_query_results(ApiQuery=api_query_latlon, ApiKey=api_key, UnitGroup=unit_group, Include=include_time
                                    ,Elements=columns_select,ContentType=file_type, StartDate=start_date, EndDate=end_date)

        # Se transforma el objeto csv retornado a dataframe de pandas
        df_rain = pd.read_csv(StringIO(body_rain.decode('utf-8')))
        # Si aquel día tiene tipo de precipitación = 'rain' entonces se concatena al dataframe que consolida todos los resultados
        if 'rain' in df_rain['preciptype'][0]: 
            df_rain_total = pd.concat([df_rain_total,df_rain], ignore_index=True)
        else: continue
    except Exception as e:
        print('[Error]', e)
        sys.exit()

# Se calcula el promedio de las temperaturas obtenidas para aquellas visitas realizadas y consultadas a la API
temp_mean_rain = df_rain_total.temp.mean().round(2)
print('Respuesta:', temp_mean_rain,'ºC')


#########################################################################################
# ¿Cuál es la temperatura promedio para las visitas realizadas en la localidad de Suba? #
#########################################################################################
print('\n¿Cuál es la temperatura promedio para las visitas realizadas en la localidad de Suba?')

# Tomo las propiedades que corresponden a la localidad de Suba y luego me quedo con las visitas realizadas a esas propiedades
props_id_suba = df_props[df_props.localidad=='Suba'].property_id
visits_suba = df_visits[(df_visits.property_id.isin(props_id_suba)) & (df_visits.status=='Done')]

# Se arma una lista con cada conjunto de latitud, longitud, fecha inicio visita y fecha final visita por cada 
# visita realizada a esas propiedades de la localidad de Suba
latlon_props_suba = list(zip(df_props[df_props.property_id.isin(props_id_suba.unique())]['latitude']
                            ,df_props[df_props.property_id.isin(props_id_suba.unique())]['longitude']
                            ,visits_suba.begin_date.str.split('T').str.get(0)
                            ,visits_suba.end_date.str.split('T').str.get(0)))

df_suba_total = pd.DataFrame()
# Por cada conjunto de datos de la lista anterior se hace una consulta a la API
for lat, lon, start_date, end_date in latlon_props_suba:
    try:
        api_query_latlon=base_url
        # Append the latitude and longitude
        api_query_latlon+=str(lat)+","+str(lon)
        # Set columns necessary
        columns_select='datetime,temp'
        body_suba = api_query_results(ApiQuery=api_query_latlon, ApiKey=api_key, UnitGroup=unit_group, Include=include_time
                                    ,Elements=columns_select,ContentType=file_type, StartDate=start_date, EndDate=end_date)
        
        # Se transforma la respuesta del json a dataframe de pandas
        df_suba = pd.read_csv(StringIO(body_suba.decode('utf-8')))
        # Se concatena cada dataframe generado de cada consulta a la API a un dataframe que consolida los resultados
        df_suba_total = pd.concat([df_suba_total,df_suba], ignore_index=True)
    except Exception as e:
        print('[Error]', e)
        sys.exit()

# Se calcula el promedio de las temperaturas obtenidas para las visitas realizadas en la localidad de Suba
temp_mean_suba = df_suba_total.temp.mean().round(2)
print('Respuesta:', temp_mean_suba,'ºC')


# Se calcula el tiempo de ejecución del código dentro del script
print('\n\n[INFO] Tiempo ejecución script: ', str(timedelta(seconds=(time.time()-time_init))))

print('\n[THANKS] Muchas gracias por la oportunidad. Cualquier duda estaré atento a su contacto.')