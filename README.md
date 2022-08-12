# Houm Challenge
## Houm es una startup que permite administrar, arrendar y vender propiedades rápido, seguro y fácil a miles de usuarios en Latinoamérica.

#### Este repositorio contiene los archivos utilizados para ejecutar el script en Python, el cual responde a las preguntadas presentadas en el desafío.

### Datasets:
- *properties.csv*: 
Contiene información básica de las características de la propiedad y su información geográfica. Las columnas de este archivo son las siguientes: property_id, business_type, type, bedrooms, bathrooms, latitude, longitude, locality, city & country.

- *users.csv*: 
Contiene la información de los propietarios y su relación con la propiedad. Las columnas de este archivo son las siguientes: property_id, user_id, name, last_name & country.

- *visits.csv*: 
Contiene la información de los clientes que se han registrado en alguna visita a una propiedad. Las columnas de este archivo son las siguientes: schedule_id, property_id, begin_date, end_date, type_visit & status. Este ultimo campo, puede tomar valor según el estado de la visita y permite verificar si las visitas están agendadas y aún no se realizan (scheduled), canceladas (cancelled) o ya realizadas (Done)

- *Weather API de Visual Crossing (https://www.visualcrossing.com/weather-api)*:
API a la cual se le puede consultar el clima de un lugar en un rango de fechas. Esta API retorna como respuesta datos de temperatura, humedad, condiciones, entre otros.

### Código:
- *config.json*:
Archivo donde se configura la API Key entregada a una cuenta generada en el portal de Visual Crossing: https://www.visualcrossing.com/account

- *challengeHoum.py*:
Archivo python donde se desarrolla el desafío como tal, respondiendo a las preguntas entregadas.Las librerías a utilizar son:
  - json
  - time
  - datetime
  - requests
  - pandas
  - sys
  
### Ejemplo ejecución: 
`python3 challengeHoum.py`
