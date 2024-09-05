import os
from datetime import date
import requests
import pandas as pd
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq

# Fuente: BANCO CENTRAL
SOURCE = 'https://si3.bcentral.cl/Siete/es/Siete/API?respuesta='

# Variables del proyecto GCP
TABLA_DESTINO = os.getenv("bq_table",'xxxxx')
DATA_SET = os.getenv("bq_dataset",'xxxxxx')
PROYECTO = os.getenv("bq_project",'xxxxxx')

TABLA_DESTINO = f"{DATA_SET}.{TABLA_DESTINO}"

# Configurar pais y formularios de APIS
PAIS_USD_TO_LIST = {"CHILE": "F073.TCO.PRE.Z.D","PERU": "F072.PEN.USD.N.O.D","COLOMBIA": "F072.COP.USD.N.O.D"}

# Formularios para tipo de cambio de moneda extranjera a clp, por el momento no es necesario
# PAIS_CLP_TO_LIST = {"CHILE": "NONE","PERU": "F072.CLP.PEN.N.O.D","COLOMBIA": "F072.CLP.COP.N.O.D"} 

# Función para obtener la tasa de cambio del dólar a otras monedas
def get_usd_to_exchange_rate(country_code):
        user = os.getenv("user")
        pss = os.getenv("pass")

        url_usd = f"https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx?user={user}&pass={pss}&firstdate={date.today()}&lastdate={date.today()}&timeseries={country_code}&function=GetSeries"
        response = requests.get(url_usd)
        if response.status_code == 200:
                data = response.json()

                # Obtenemos el valor del dólar respecto a la moneda pais
                try:
                        usd_to_rate = float(data['Series']['Obs'][0]['value'])
                except:
                        usd_to_rate = None
                return usd_to_rate
        else:
                # Si la solicitud no fue exitosa, imprimimos un mensaje de error
                print(f'Error: No se pudo obtener el valor del dólar respecto a la moneda del pais {country_code}.')
                return None

# Iterar sobre cada país y su moneda correspondiente
def main():
        for country, formulario in PAIS_USD_TO_LIST.items():
                # Verificar si hoy existe data
                QUERY = f"SELECT FECHA FROM `{PROYECTO}.{TABLA_DESTINO}` WHERE FECHA = DATE '{date.today()}' AND PAIS = '{country}'"
                QUERY_RESULT = read_gbq(QUERY, project_id=PROYECTO, progress_bar_type=None)
                
                if QUERY_RESULT.empty:

                        # Obtener la tasa de cambio
                        USD_TO = get_usd_to_exchange_rate(formulario)

                        # Si el valor es nulo tomar los valores del dia anterior
                        if USD_TO == None:
                                USD_TO_QUERY = f"SELECT DISTINCT CAST(DOLAR_A_MONEDA_PAIS_ULT_VALOR AS FLOAT64) FROM `{PROYECTO}.{TABLA_DESTINO}` WHERE FECHA = DATE '{date.today()}' - 1 AND PAIS = '{country}'"
                                R_USD_TO = read_gbq(USD_TO_QUERY,project_id=PROYECTO,progress_bar_type=None)
                                R_USD_TO = float(R_USD_TO['f0_'].iloc[0])
                        else:
                                R_USD_TO = USD_TO

                        # Calcular el tipo de cambio a CLP
                        if country != "CHILE":
                                USD_TO_COUNTRY_CODE = PAIS_USD_TO_LIST["CHILE"] 
                                USD_TO_CLP = get_usd_to_exchange_rate(USD_TO_COUNTRY_CODE)
                                if USD_TO_CLP != None:
                                        TIPO_CAMBIO_TO_CLP = (1 / USD_TO) * USD_TO_CLP
                                        R_TIPO_CAMBIO_TO_CLP = TIPO_CAMBIO_TO_CLP
                                else:
                                        # Si el valor es nulo tomar los valores del dia anterior

                                        USD_TO_CLP_QUERY =f"SELECT DISTINCT CAST(DOLAR_A_MONEDA_PAIS_ULT_VALOR AS FLOAT64) FROM `{PROYECTO}.{TABLA_DESTINO}` WHERE FECHA = DATE '{date.today()}' - 1 AND PAIS = 'CHILE'"
                                        R_USD_TO_CLP = read_gbq(USD_TO_CLP_QUERY,project_id=PROYECTO,progress_bar_type=None)
                                        R_USD_TO_CLP = float(R_USD_TO_CLP['f0_'].iloc[0])
                                        
                                        R_TIPO_CAMBIO_TO_CLP = (1 / R_USD_TO) * R_USD_TO_CLP
                        else:
                                if USD_TO != None:
                                        TIPO_CAMBIO_TO_CLP = 1.0
                                        R_TIPO_CAMBIO_TO_CLP = TIPO_CAMBIO_TO_CLP
                                else:
                                        TIPO_CAMBIO_TO_CLP = None
                                        R_TIPO_CAMBIO_TO_CLP = 1.0
                                
                        # Configurar el dataframe por pais
                        df = pd.DataFrame({"PAIS": [country],"FECHA": [date.today()],"DOLAR_A_MONEDA_PAIS": [USD_TO],"DOLAR_A_MONEDA_PAIS_ULT_VALOR": [R_USD_TO]
                                        ,"TIPO_CAMBIO_A_CLP": [TIPO_CAMBIO_TO_CLP],"TIPO_CAMBIO_A_CLP_ULT_VALOR": [R_TIPO_CAMBIO_TO_CLP],
                                        "SOURCE": [SOURCE]})
                        schema = [
                                {"name": "PAIS","type": "STRING"},
                                {"name": "FECHA","type": "DATE"},
                                {"name": "DOLAR_A_MONEDA_PAIS","type": "FLOAT"},
                                {"name": "DOLAR_A_MONEDA_PAIS_ULT_VALOR","type": "FLOAT"},
                                {"name": "TIPO_CAMBIO_A_CLP","type": "FLOAT"},
                                {"name": "TIPO_CAMBIO_A_CLP_ULT_VALOR","type": "FLOAT"},
                                {"name": "SOURCE","type": "STRING"}
                                ]

                        # Escribir el DataFrame en BigQuery
                        to_gbq(df, destination_table=TABLA_DESTINO, project_id=PROYECTO, if_exists='append',table_schema=schema)
                        
                        print(f'Se ha cargado la data para el pais {country} en la fecha {date.today()}'
                                , f''' los valores para ese dia son: \n
                                        DOLAR_A_MONEDA_PAIS: {USD_TO} \n
                                        TIPO_CAMBIO_A_CLP: {TIPO_CAMBIO_TO_CLP}
                                        DOLAR_A_MONEDA_PAIS_ULT_VALOR: {R_USD_TO} \n
                                        TIPO_CAMBIO_A_CLP_ULT_VALOR: {R_TIPO_CAMBIO_TO_CLP}''')

                else:
                        print(f'El pais {country} ya cuenta con data para el dia de hoy')

if __name__ == "__main__":
    main()
