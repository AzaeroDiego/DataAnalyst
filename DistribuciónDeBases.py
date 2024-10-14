#Script para analizar BBDD que envía área de Ventas y se realiza prospección 

import pandas as pd
from datetime import datetime
from datetime import date
import time
import os
from datetime import datetime
import numpy as np
import glob # Para leer archivos de una carpeta
import xlrd
import openpyxl
import locale
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import re
import warnings

# Suprimir las advertencias de tipo FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)


# Configurar locale para español (Perú)
locale.setlocale(locale.LC_TIME, 'es_PE.UTF-8')
usuario = 'azaer'

hoy = date.today().strftime('%Y%m%d')
periodo = date.today().strftime('%Y%m')
periodo = '202409'
inicio_mes = f'{periodo}' + '01'
mes = datetime.strptime(inicio_mes, '%Y%m%d').strftime('%B')
print(mes)

ruta_bases = rf'C:\Users\{usuario}\Documents\Diego\BASES\{mes}\\'
nombre_archivo = f'TotalBases{mes}.xlsx'
df_clientesJunio = pd.read_excel(ruta_bases + nombre_archivo, sheet_name='Hoja1',dtype=str)
print(df_clientesJunio.columns)

#Analizar bases importadas


print(df_clientesJunio.info())
print(df_clientesJunio.isnull().sum())
print(df_clientesJunio.describe())


# Respaldar el df

df = df_clientesJunio.copy()

#Algun Filtro si se requiere (Eliminar si columna grupo es igual a Grupo1)
df = df[df['Obs'] == 'refil']

# Añadir Columna de Zona (Si es Lima,provincia o callao a partir de columna departamento)
df['Zona2'] = df['provincia'].apply(lambda x: 'lima' if x in ['lima', 'callao'] else 'provincia')


df['numero_documento'] = df['numero_documento'].str[-8:]


# Añadir columna [RangoEdad] a partir de la columna edad 

df['edad'] = df['edad'].astype(int)
bins = [18, 30, 40, 50 ,100]
labels = ['20-29', '30-39', '40-49', '>50']
df['RangoEdad'] = pd.cut(df['edad'], bins=bins, labels=labels, right=False)

# Determinar los conos de Lima
conos = {
    'CONO NORTE': ['ancon', 'puente piedra', 'santa rosa', 'carabayllo', 'comas', 'los olivos', 'independencia', 'san martin de porres', 'rimac'],
    'CONO ESTE': ['san juan de lurigancho', 'santa anita', 'cieneguilla', 'ate', 'chaclacayo', 'lurigancho', 'el agustino'],
    'CONO SUR': ['san juan de miraflores', 'villa maria del triunfo', 'villa el salvador', 'lurin', 'pachacamac','chorrillos'],
    'CENTRAL': ['lima', 'san luis', 'breña', 'la victoria','callao','bellavista'],
    'ALTO NIVEL': ['barranco', 'jesus maria', 'la molina', 'lince', 'magdalena del mar', 'miraflores', 'pueblo libre', 'san borja', 'san isidro', 'san luis', 'san miguel', 'santiago de surco', 'surquillo','la punta'],
    'Otros': ['punta hermosa', 'pucusana', 'punta negra', 'san bartolo', 'santa maria','asia','mala','santa maria del mar'],
    'LIMA PROVINCIAS' : ['barranca', 'cajatambo', 'cañete','san vicente de cañete', 'canta', 'huaral', 'huarochiri', 'huaura', 'oyon', 'yauyos','santa rosa de quives'],
}

segmentacion_provincia = {
    'Norte_Provincia' : ['tumbes', 'piura', 'lambayeque','la libertad', 'ancash', 'cajamarca', 'san martin', 'amazonas'],
    'Centro_Provincia' : ['huanuco', 'pasco', 'junin',  'madre de dios','ucayali','loreto'],
    'Sur_Provincia' : ['ica', 'ayacucho', 'apurimac', 'cusco', 'puno', 'arequipa', 'moquegua', 'tacna','huancavelica']
}

#convertir minuscula los distritos para hacer match, reemplazar áéíóú por aeiou
df['distrito'] = df['distrito'].str.lower()

#Reemplazar caracteres acentuados en todo el dataframe 
df = df.replace({'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u'}, regex=True)


# Función para determinar el Cono o la Provincia
def determinar_cono(row):
    if (row['departamento'] == 'lima' and row['provincia'] == 'lima') or row['provincia'] == 'callao':
        for cono, distritos in conos.items():
            if row['distrito'] in distritos:
                return cono
        return 'Otros'
    elif row['departamento'] == 'lima':
        return 'LIMA PROVINCIAS'
    else:
        for region, provincias in segmentacion_provincia.items():
            if row['departamento'] in provincias:
                return region
    return 'Otros'

# Aplicar la función
df['Cono'] = df.apply(determinar_cono, axis=1)

# Convertir la columna 'Fecha_colocacion' a formato datetime
df['fecha_colocacion2'] = pd.to_datetime(df['fecha_colocacion'], format='%d/%m/%y', dayfirst=True, errors='coerce').fillna(inicio_mes)

#convertir la variable en datatime
inicio_mes = datetime.strptime(inicio_mes, '%Y%m%d')

# Calcular la fecha de tres meses antes de inicio_mes
fecha_limite = inicio_mes - relativedelta(months=3)

# Función para determinar la recencia
def determinar_recencia(row):
    if pd.isnull(row['recencia']) and row['fecha_colocacion2'] > fecha_limite:
        return 'Sin Recencia'
    elif pd.isnull(row['recencia']) and row['fecha_colocacion2'] <= fecha_limite:
        return 'NO USA TARJETA'
    elif row['recencia'] == str(int(periodo) - 1) or row['recencia'] == str(int(periodo)):
        return 'RECENCIA 1 MES'
    elif row['recencia'] == str(int(periodo) - 2):
        return 'RECENCIA 2 MESES'
    else:
        return 'RECENCIA 3+'


# Aplicar la función a cada fila
df['Recencia'] = df.apply(determinar_recencia, axis=1)


# añadir tipo_captación a partir de la columna tipo_base
if 'tipo_base' in df.columns:
    df['tipo_captacion'] = df['tipo_base'].map({'cross_asociado': 'cross asociado', 'cross_no_asociado': 'cross no asociado', 'welcome_apertura': 'welcome'}).fillna('stock')
    

df['Campaña2'] = df['Campaña'].map({'sonrieseguro-callsouth': 'sonrie seguro', 'caminaseguro-callsouth': 'camina seguro'}).fillna('AP')

# Convertir las fechas a datetime
df['fecha_compra_ultimo_seguro'] = pd.to_datetime(df['fecha_compra_ultimo_seguro'], format='%d/%m/%y', errors='coerce')
df['fecha_ult_consumo'] = pd.to_datetime(df['fecha_ult_consumo'], format='%d/%m/%y', errors='coerce')
df['fecha_colocacion'] = pd.to_datetime(df['fecha_colocacion'], format='%d/%m/%y', errors='coerce')

# Rellenar NaNs con una fecha muy antigua
fecha_antigua = pd.to_datetime('2023-01-01')
df['fecha_compra_ultimo_seguro'].fillna(fecha_antigua, inplace=True)
df['fecha_ult_consumo'].fillna(fecha_antigua, inplace=True)
df['fecha_colocacion'].fillna(fecha_antigua, inplace=True)

# Asegurarse de que las columnas numéricas estén en tipo float
df['cantidad_seguros'] = df['cantidad_seguros'].astype(float).fillna(0)

# Asegurar que todos los valores en seguros_contratados sean cadenas de texto
df['seguros_contratados'] = df['seguros_contratados'].astype(str).fillna('')

#Normalizar seguros que tenga contratado el cliente (Producto Ripley)
df['filter_seguros_contratados'] = df['producto_ripley'].map({
    'protección de tarjeta full': 'protección de tarjeta',
    'protección de tarjeta full digital': 'protección de tarjeta',
    'proteccion de tarjetas plus': 'protección de tarjeta',
    'proteccion de tarjetas plus c': 'protección de tarjeta',
    'seguro vida pya': 'seguro de vida',
    'seguro vida ripley': 'seguro de vida',
    'seguro vida sef': 'seguro de vida',
    'seguro vida sef sw': 'seguro de vida',
    'seguro vida total': 'seguro de vida',
    'seguro de sepelio c': 'seguro de sepelio',
    'seguro de sepelio mapfre': 'seguro de sepelio',
    'seguro sepelio plan 2': 'seguro de sepelio',
    'sepelio full': 'seguro de sepelio',
    'seguro de accidentes personales e invalidez mapfre': 'seguro de accidentes',
    'seguro vehicular rimac tlmk': 'seguro vehicular',
    'multiproteccion de tarjeta de credito ripley m': 'multiprotección de tarjeta',
    'multiproteccion de tarjeta de credito ripley telemarketing': 'multiprotección de tarjeta'}).fillna(df['producto_ripley'])

# Crear una función de puntuación
def calcular_puntuacion_fecha(row):
    puntos = 0
    
    # Puntuación basada en la fecha de compra del último seguro (más reciente es mejor)
    delta_ultimo_seguro = (inicio_mes - row['fecha_compra_ultimo_seguro']).days
    puntos += max(0, 365 - delta_ultimo_seguro) / 365
    
    # Puntuación basada en la cantidad de seguros contratados
    puntos += row['cantidad_seguros'] * 0.1
    
    # Puntuación basada en la fecha del último consumo (más reciente es mejor)
    delta_ult_consumo = (inicio_mes - row['fecha_ult_consumo']).days
    puntos += max(0, 180 - delta_ult_consumo) / 180
    
    # Puntuación basada en la fecha de colocación (más reciente es mejor)
    delta_colocacion = (inicio_mes - row['fecha_colocacion']).days
    puntos += max(0, 365 - delta_colocacion) / 365
    
    return puntos

# Aplicar la puntuación a cada cliente
df['puntuacion_fechas'] = df.apply(calcular_puntuacion_fecha, axis=1)

# Crear la función de puntuación basada en los tipos de seguros contratados
def calcular_puntuacion_seguros(row):
    puntos = 0
    seguros = row['seguros_contratados'].split('+')
    
    # Expresiones regulares para buscar palabras clave
    proteccion_tarjeta_regex = re.compile(r'proteccion de? tarjetas?')
    accidentes_personales_regex = re.compile(r'accidentes personales?')
    
    if any(proteccion_tarjeta_regex.search(seguro) for seguro in seguros):
        puntos += 0.1
    if any(accidentes_personales_regex.search(seguro) for seguro in seguros):
        puntos += 0.05
    
    return puntos

# Aplicar la puntuación de seguros a cada cliente
df['puntuacion_seguros'] = df.apply(calcular_puntuacion_seguros, axis=1)

                                       
# Determinar porcentajes para Edad (Equivale 30%)
df['PorcentajeEdad'] = 0.0
df.loc[df['RangoEdad'] == '20-29', 'PorcentajeEdad'] = 0.501986716
df.loc[df['RangoEdad'] == '30-39', 'PorcentajeEdad'] = 0.22291946
df.loc[df['RangoEdad'] == '40-49', 'PorcentajeEdad'] = 0.134103768
df.loc[df['RangoEdad'] == '>50', 'PorcentajeEdad'] = 0.140990056


# Determinar porcentaje para Recencia (Equivale 40%)
df['porcentaje_recencia'] = 0.0
df.loc[df['Recencia'] == 'Sin Recencia', 'porcentaje_recencia'] = 0.15
df.loc[df['Recencia'] == 'NO USA TARJETA', 'porcentaje_recencia'] = -0.20
df.loc[df['Recencia'] == 'RECENCIA 1 MES', 'porcentaje_recencia'] = 0.65
df.loc[df['Recencia'] == 'RECENCIA 2 MESES', 'porcentaje_recencia'] = 0.20
df.loc[df['Recencia'].isnull(), 'porcentaje_recencia'] = 0.001

# Determinar porcentaje para Zona (Equivale 30%)

df['Cono'] = df['Cono'].fillna('')  # Llenar NaN con una cadena vacía

df['porcentaje_zona'] = 0.0
df.loc[df['Cono'].str.contains('Provincia'), 'porcentaje_zona'] = 0.5347
df.loc[df['Cono'] == 'Otros', 'porcentaje_zona'] = 0.0372
df.loc[df['Cono'].str.contains('CONO'), 'porcentaje_zona'] = 0.2997
df.loc[df['Cono'] == 'ALTO NIVEL', 'porcentaje_zona'] = 0.0856
df.loc[df['porcentaje_zona'].isnull(), 'porcentaje_zona'] = 0.0428

# Determinar porcentaje Total
df['Porcentaje'] = df['PorcentajeEdad'] * 0.3 + df['porcentaje_recencia'] * 0.4 + df['porcentaje_zona'] * 0.3

#Sumar todos los porcentajes + puntuacion de fechas + puntuacion de seguros
df['Porcentaje'] = df['Porcentaje'] + df['puntuacion_fechas'] * 0.1 + df['puntuacion_seguros']

# Importar Scored de Chubb
excel_chubb = f'C:\\Users\\{usuario}\\Downloads\\2024_08_30_Scored Callsouth.xlsx'
df_chubb = pd.read_excel(excel_chubb, sheet_name='Sheet1', header=0, dtype=str)


#Cruzar por DNI y reemplazar Categoria y Calificacion por columnas Score y Decile cruzando numero_documento
df = df.merge(df_chubb[['numero_documento', 'Score', 'Decile','Category']], on='numero_documento', how='left')
df['Porcentaje'] = pd.to_numeric(df['Category'], errors='coerce').fillna(df['Porcentaje'])


# Asignar deciles usando pd.qcut con el argumento duplicates='drop'
df['Decil'] = pd.cut(df['Porcentaje'], bins=10, labels=False)

# Según decil determinar Bajo, Medio, Alto y asignar calificación numérica
decil_map = {
    0: ('Bajo', 10),
    1: ('Bajo', 9),
    2: ('Bajo', 8),
    3: ('Medio Bajo', 7),
    4: ('Medio Bajo', 6),
    5: ('Regular', 5),
    6: ('Medio Alto', 4),
    7: ('Medio Alto', 3),
    8: ('Alto', 2),
    9: ('Alto', 1)
}
df['Categoria'] = df['Decil'].map(lambda x: decil_map[x][0])
df['Calificacion'] = df['Decil'].map(lambda x: decil_map[x][1])

df['Decil'] = df['Categoria'] 

# Añadir Columna de Marca BBDD con la información relevante ('Bloq Carterizar Abr + tipo_captacion + Zona2 + Rango Edad +  Decil')
df['Marca_BBDD'] = f'Carterizar {mes}_' + df['Campaña2'] + '_' + df['tipo_captacion'].astype(str) + '_' + df['Zona2'].astype(str) + '_' + df['RangoEdad'].astype(str) + '_' + df['Decil'].astype(str)


# Exportar a Formato Deseado
df_export = pd.DataFrame({
    'Carga' : 'Carga_' + df['Campaña2'] + '_' + mes ,
    'numero_documento': df['numero_documento'].str[-8:],
    'tipo_documento': '1',
    'nombre': df['nombre'],
    'tipo_tarjeta': df['tipo_tarjeta'],
    'fecha_nacimiento' : '',
    'edad': df['edad'],
    'sexo': df['sexo'],
    'celular1': df['celular1'],
    'celular2': df['celular2'],
    'fijo1': '',
    'fijo2': '',
    'email': df['email'],
    'direccion': df['direccion'],
    'distrito': df['distrito'],
    'provincia': df['provincia'],
    'departamento': df['departamento'],
    'fecha_colocacion': df['fecha_colocacion'].dt.strftime('%d/%m/%Y'),
    'tienda_colocacion': df['tienda_colocacion'],
    'fecha_ult_consumo': df['fecha_ult_consumo'].dt.strftime('%d/%m/%Y'),
    'tienda_ult_consumo': df['tienda_ult_consumo'],
    'condicion_laboral': df['condicion_laboral'],
    'zona' : df['Zona2'],
    'recencia': df['recencia'],
    'cantidad_seguros': df['cantidad_seguros'],
    'seguros_contratados': df['seguros_contratados'],
    'clase_puntos_beneficios': df['clase_puntos_beneficios'],
    'producto_ripley': df['producto_ripley'],
    'fecha_compra_ultimo_seguro': df['fecha_compra_ultimo_seguro'].dt.strftime('%d/%m/%Y'),
    'tiene_seguro': df['tiene_seguro'],
    'fecha_pago_tc': df['fecha_pago_tc'],
    'marca_call': df['marca_call'],
    'marca_pd' : df['marca_pd'], 
    'canal_consumo': df['canal_consumo'],
    'segmento_rfm_spos': df['segmento_rfm_spos'],
    'segmento_rfm_tienda': df['segmento_rfm_tienda'],
    'tipo_captacion': df['tipo_captacion'],
    'nombre_call': 'callsouth',
    'tipo_base': df['tipo_captacion'],
    'Campana': df['Campaña2'],
    'FILTER_RANGO_EDAD' :  df['RangoEdad'],
    'FILTER_SEXO' : df['sexo'],
    'FILTER_PROVINCIA' :  df['Zona2'],
    'FILTER_CONO' : df['Cono'],
    'FILTER_RECENCIA' : df['Recencia'],
    'FILTER_FECHA_PAGO' : df['fecha_pago_tc'],
    'FILTER_Puntuacion' : df['Porcentaje'].round(2),
    'FILTER_PRODUCTO_RIPLEY' : df['seguros_contratados'].fillna(''),
    'FILTER_SUBTIPOBASE' : df['tipo_captacion'],
    'FILTER_calificacion' : df['Decil'],
     'CampoCarterizar' : df['Marca_BBDD'] ,
     'Filtro_Fecha' :'',
     'Filtro_Grupo' : '',
        'FILTER_SUBCARGA' : 'Carga_'+ df['Campaña2'] + '_' f'{hoy}', 
     'Filtro_4' : '',
        'Filtro_5' : '',
        'Filtro_6' : '',
        'Filtro_7' : '',
        'Filtro_8' : '',
        'Filtro_9' : '',
        'Filtro_10' : '',
        'Agente' : '',

})

#Ordenar registros por SUBTIPOBASE (Primero Welcome, luego Cross luego stock), luego Porcentaje (Mayor a menor)

df = df_export

# Exportar a Excel 
nombre_archivo = f'Clientes_{mes}_{hoy}_segmentado.xlsx'
df.to_excel(ruta_bases + nombre_archivo, index=False)


