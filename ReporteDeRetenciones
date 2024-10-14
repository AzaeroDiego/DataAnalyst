import pandas as pd
import datetime
import os
import numpy as np
import glob # Para leer archivos de una carpeta
import xlrd
import openpyxl
import xml.etree.ElementTree as ET
import locale
from pandas.io.formats.excel import ExcelFormatter
import matplotlib.pyplot as plt
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy import create_engine, text
from dateutil.parser import parse
import time
from datetime import datetime
from rapidfuzz import process, fuzz
import difflib

# Configurar locale para español (Perú)
locale.setlocale(locale.LC_TIME, 'es_PE.UTF-8')

# Variables 
fecha = '202408'
# Mes a partir del fecha
mes = datetime.strptime(fecha, '%Y%m').strftime('%B').capitalize()
usuario = 'azaer'
Ruta = rf'C:\Users\{usuario}\Documents\Diego\Reportes'
Ruta_Edson = rf'C:\Users\azaer\OneDrive - CALLSOUTH S A\Reportes_Ventas'
hoy = datetime.now().strftime('%Y%m%d_%H%M')

# Definir los parámetros de conexión
server = r'server'
database = 'database'
schema = 'dbo'
username = 'user'
password = 'password'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'

# Crear el motor de conexión
engine = create_engine(f'mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')

# Crear la conexión
connection = pyodbc.connect(connection_string)

# Realizar la importación del archivo xls más reciente de la carpeta de descargas
carpeta_descargas = rf'C:\Users\{usuario}\downloads'
# Lista de archivos xls de la carpeta de descargas
archivos_xls = glob.glob(os.path.join(carpeta_descargas, '*.xls'))
# Obtener el archivo más reciente
archivo_mas_reciente = max(archivos_xls, key=os.path.getmtime) if archivos_xls else None


# Leer el archivo XML
tree = ET.parse(archivo_mas_reciente)
root = tree.getroot()

# Namespace para las etiquetas de Excel
ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}

# Encontrar la hoja "Consulta Producción"
worksheet = None
for ws in root.findall('.//ss:Worksheet', ns):
    name = ws.attrib.get('{urn:schemas-microsoft-com:office:spreadsheet}Name')
    if name == 'Consulta Producción':
        worksheet = ws
        break

if worksheet is None:
    raise ValueError("No se encontró la hoja 'Consulta Producción'")

# Extraer el nodo de la tabla
table = worksheet.find('.//ss:Table', ns)

# Inicializar una lista para almacenar los datos
data = []

# Contador de filas para saltar las primeras 7
row_count = 0

# Iterar sobre las filas de la tabla
for row in table.findall('.//ss:Row', ns):
    # Saltar las primeras 7 filas
    if row_count < 7:
        row_count += 1
        continue
    
    # Inicializar una lista para almacenar los datos de las celdas
    row_data = []
    for cell in row.findall('.//ss:Cell', ns):
        # Obtener el valor de la celda
        data_element = cell.find('.//ss:Data', ns)
        cell_value = data_element.text if data_element is not None else ''
        row_data.append(cell_value)
    data.append(row_data)

# Convertir los datos a un DataFrame
df = pd.DataFrame(data)

# Asignar la primera fila como encabezado
df.columns = df.iloc[0]
df_global = df[1:]

# Resetear los índices del DataFrame
df_global.reset_index(drop=True, inplace=True)

carpeta_descargas = rf'C:\Users\{usuario}\downloads'
# Lista de archivos xlsx de la carpeta de descargas que contenga el nombre 'Trama Unica'
archivos_xlsx = glob.glob(os.path.join(carpeta_descargas, '*Trama Unica*.xlsx'))
# Obtener el archivo más reciente
archivo_mas_reciente = max(archivos_xlsx, key=os.path.getmtime) if archivos_xlsx else None


# Leer el archivo xlsx
df_trama = pd.read_excel(archivo_mas_reciente, sheet_name=0, header=0,dtype=str)

#Cambiar la SUCURSAL ANULACION a 'TLMK RETENCIÓN' para el df_global con DNI 47226869
df_global.loc[df_global['N° DOCUMENTO TITULAR DE CUENTA'].isin(['73108216','45828700'])  , 'SUCURSAL ANULACION'] = 'TLMK RETENCIÓN'

#Filtrar 'SUCURSAL ANULACION = 'TLMK RETENCIÓN o TELEMARKETING
df_global = df_global[(df_global['SUCURSAL ANULACION'] == 'TLMK RETENCIÓN') | (df_global['SUCURSAL ANULACION'] == 'TELEMARKETING')]


#Eliminar registros de df_global si Funcionario Anula es igual a 'KATHERIN ESTEFANIA ABAD ROMERO'
df_global = df_global[df_global['FUNCIONARIO ANULA'] != 'KATHERIN ESTEFANIA ABAD ROMERO']

# Crear diccionario de mapeo
mapeo_nombres = {
    "Pepe" : "Pepito",
    "Pedro" : "Pedrito",
    "Información de" : "Prueba"
}

mapeo_seguros = {
    'PROTECCIÓN DE TARJETA FULL': 'SEGURO DE PROTECCION DE TARJETAS FULL',
    'RIPLEY SONRIE SEGURO': 'Sonríe Seguro CHUBB',
    'SEGURO DE ACCIDENTES PERSONALES E INVALIDEZ MAPFRE': 'Accidentes Personales con Asistencias MAPFRE',
    'ACCIDENTES PERSONALES INDIVIDUAL RIPLEY' : 'Accidentes Personales con Asistencias MAPFRE',
    'PROTECCION DE TARJETAS PLUS': 'PROTECCION DE TARJETAS PLUS MAPFRE',
    'SEPELIO FULL': 'SEGURO SEPELIO FULL',
    'SONRISA PROTEGIDA RIPLEY': 'SONRISA PROTEGIDA',
    'SEGURO VIDA TOTAL': 'SEGURO VIDA TOTAL',
    'SEGURO DENTAL RIPLEY': 'Sonríe Seguro CHUBB',
    'AHORRO SEGURO': 'Otros',
    'RIPLEY PROTECCIÓN ACCIDENTAL': 'Protección Accidental con Asistencias Integrales CHUBB',
    'SEGURO SEPELIO PLAN 2': 'SEGURO SEPELIO PLAN 2',
    'PROTECCIÓN DE TARJETA FULL DIGITAL': 'SEGURO DE PROTECCION DE TARJETAS FULL',
    'SEGUROS DE DESEMPLEO Y EVENTOS VIDA': 'Otros',
    'RIPLEY CAMINA SEGURO': 'Camina Seguro CHUBB',
    'SEGURO DE SEPELIO MAPFRE': 'SEPELIO MV',
    'PROTECCIÓN DE PAGOS SUPER EFECTIVO': 'PROTECCIÓN PAGOS SEF',
    'MULTIPROTECCION DE TARJETA DE CREDITO RIPLEY M': 'Otros',
    'SEGURO VIDA SEF SW': 'SEGURO DE VIDA SEF FULL',
    'SEGURO ACCIDENTES PERSONALES CON ASISTENCIAS': 'Accidentes Personales con Asistencias MAPFRE',
    'NUEVOVIDAEFEX': 'SEGURO VIDA EFEX',
    'PROTECCIÓN DE PAGOS': 'PROTECCIÓN PAGOS SEF',
    'ASISTENCIA M': 'Otros',
    'ASISTENCIA  M' : 'Otros',
    'ACCIDENTES PERSONALES RIPLEY': 'Accidentes Personales con Asistencia Dental RIMAC'
}

# Normalizar los nombres en df_global
df_global['FUNCIONARIO ANULA'] = df_global['FUNCIONARIO ANULA'].map(mapeo_nombres)

# Combinar columna de Fecha y hora fecha formato inicial DD/MM/YYYY y Hora HH:MM
df_global['FECHA DE ANULACIÓN'] = df_global['FECHA DE ANULACIÓN'] + ' ' + df_global['HORA DE ANULACIÓN'] 

# Combinar Fecha y hora en df_trama
df_trama['FECHA'] = df_trama['FECHA'] + ' ' + df_trama['HORA FIN AGENTE']

# Normalizar seguros Global por los del mapeo
df_global['PRODUCTO'] = df_global['PRODUCTO'].map(mapeo_seguros)

#Anular Bajas de Global que estén en hoja
eliminar_registros = pd.read_excel(os.path.join(Ruta_Edson, 'Ingresar Gestiones CRM.xlsx'), sheet_name="Eliminar", header=0,dtype=str)
#Eliminar registros de df_trama que coincida con ID PÓLIZA
df_global = df_global[~df_global['ID PÓLIZA'].isin(eliminar_registros['ID PÓLIZA'])]

#Subir df_global a la base de datos Tabla Anulaciones_Global
df_global.to_sql('Anulaciones_Global', con=engine, if_exists='replace', index=False)

#Rellenar la columna NUMERO DE DOCUMENTO con 8 digitos rellenar con 0
df_trama['NUMERO DE DOCUMENTO'] = df_trama['NUMERO DE DOCUMENTO'].str.zfill(8)

#Consolidar Archivo de Edson "Añadir Gestiones"
df_edson = pd.read_excel(os.path.join(Ruta_Edson, f'Ingresar Gestiones CRM_{mes}.xlsx'), sheet_name="Agregar gestiones", header=0,dtype=str).drop(columns=['MOTIVO'])

#Eliminar espacios excesivos en todas las columnas de df_edson
df_edson = df_edson.applymap(lambda x: x.strip() if isinstance(x, str) else x)

df_edson['NUMERO DE DOCUMENTO'] = df_edson['NUMERO DE DOCUMENTO'].str.zfill(8)

#Normalizar Renuncia y retención en columna NIVEL 4
df_edson['NIVEL 4'] = df_edson['NIVEL 4'].str.capitalize()
df_edson['NIVEL 4'] = df_edson['NIVEL 4'].replace({'Retencion': 'Retención'})

df_edson['FECHA'] = pd.to_datetime(df_edson['FECHA'],format='mixed').dt.strftime('%d-%m-%Y %H:%M:%S')

#consolidar df_edson con df_trama
df_trama = pd.concat([df_trama, df_edson], ignore_index=True)

# Lista de archivos xlsx de la carpeta de descargas que contenga el nombre 'llenado de datos' y que no sea un archivo temporal
archivos_xlsx = [
    archivo for archivo in glob.glob(os.path.join(carpeta_descargas, "*LLENADO DE DATOS*.xlsx"))
    if not os.path.basename(archivo).startswith('~$')
]

# Obtener el archivo más reciente
archivo_mas_reciente = max(archivos_xlsx, key=os.path.getctime)

# Leer el archivo xlsx
df_forms = pd.read_excel(archivo_mas_reciente, sheet_name=0, header=0,dtype=str)


df_trama_nuevo = pd.DataFrame({
    'FECHA' : pd.to_datetime(df_forms['Hora de inicio'],format='mixed').dt.strftime('%d-%m-%Y %H:%M:%S'),
    'CALL ID' : df_forms['Call ID'],
    'USER NEOTEL' : '',
    'NOMBRE AGENTE' : df_forms['Nombre Agente'].str.lower(),
    'INICIO LLAMADA' : pd.to_datetime(df_forms['Hora de inicio'], format='mixed').dt.strftime('%d-%m-%Y'),
    'HORA RINGING' : '',
    'HORA INICIO AGENTE' : pd.to_datetime(df_forms['Hora de inicio'], format='mixed').dt.strftime('%H:%M:%S'),
    'HORA FIN AGENTE' : pd.to_datetime(df_forms['Hora de finalización'], format='mixed').dt.strftime('%H:%M:%S'),
    'CLI ID' : '',
    'CLI FECHA TIPIFICACION' : pd.to_datetime(df_forms['Hora de inicio'], format='mixed').dt.strftime('%d-%m-%Y'),
    'CLI HORA TIPIFICACION' : pd.to_datetime(df_forms['Hora de inicio'], format='mixed').dt.strftime('%H:%M:%S'),
    'NOMBRE PLAN' : df_forms['Nombre Plan'],
    'CIA' : '',
    'TIPO 1' : '', 
    'TIPO 2': '',
    'CUV' : df_forms['CUV'],
    'CLI NOMBRE CLIENTE' : df_forms['Nombre cliente'],
    'TIPO DE DOCUMENTO' : '1',
    'NUMERO DE DOCUMENTO' : df_forms['DNI'],
    'FECHA NACIMIENTO' : '',
    'SEXO' : df_forms['Sexo'],
    'TELEFONO 1' : df_forms['Teléfono'],
    'TELEFONO 2' : df_forms['Teléfono'],
    'CLI EMAIL' : df_forms['Correo'],
    'DEPARTAMENTO' : df_forms['Departamento'],
    'PROVINCIA' : df_forms['Provincia'],
    'DISTRITO': df_forms['Distrito'],
    'DIRECCION' : df_forms['Dirección'],
    'NIVEL 1' : df_forms['Nivel 1'],
    'NIVEL 2' : df_forms['Nivel 2'],
    'NIVEL 3' : df_forms['Nivel 3'],
    'NIVEL 4' : df_forms['Nivel 4'],
    'MODIFICADO' : '',
    'TMO' : '',
    'CALIDAD' : ''
})



#Concatenar Trama con df_trama_nuevo
df_trama = pd.concat([df_trama, df_trama_nuevo], ignore_index=True)

#Leer archivo de Google Forms

url = 'Link de Google Sheets'
url_1 = 'Link de Google Sheets 2'

#df_google_antiguo = pd.read_excel(url_1, sheet_name=0, header=0,dtype=str)
df_google = pd.read_excel(url, sheet_name=0, header=0,dtype=str)
df_google_antiguo = pd.read_excel(url_1, sheet_name=0, header=0,dtype=str)

df_google = pd.concat([df_google_antiguo, df_google], ignore_index=True)

#Eliminar espacios excesivos en todas las columnas de df_google
df_google = df_google.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Reemplazar NaN con cadenas vacías en ambas columnas antes de la concatenación
df_google['NIVEL 3'] = df_google['NIVEL_3'].fillna('') + df_google['NIVEL_3.2'].fillna('')

#Si la columna nivel4.1 no es vacío, entonces usar ese valor, caso contrario usar el valor de nivel4.2
df_google['NIVEL 4'] = df_google['NIVEL_4'].fillna('') + df_google['NIVEL_4.2'].fillna('')

#Convertir en datatime columna Marca temporal y eliminar registros de hoy
df_google['Marca temporal'] = pd.to_datetime(df_google['Marca temporal'], format='mixed')

#Filtra los registros y muestra solo los del mes actual (Contiene la variable fecha)
df_google = df_google[df_google['Marca temporal'].dt.strftime('%Y%m') == fecha]
df_google = df_google[df_google['Marca temporal'].dt.strftime('%Y%m%d') != hoy[0:8]]



df_trama_google = pd.DataFrame({
    'FECHA' : pd.to_datetime(df_google['Marca temporal'], format='mixed').dt.strftime('%d-%m-%Y %H:%M:%S'),
    'CALL ID' : df_google['Call_ID'],
    'USER NEOTEL' : '',
    'NOMBRE AGENTE' : df_google['Nombre Agente (Coloca tu nombre y un apellido)'].str.lower(),
    'INICIO LLAMADA' : pd.to_datetime(df_google['Marca temporal'], format='mixed').dt.strftime('%d-%m-%Y'),
    'HORA RINGING' : '',
    'HORA INICIO AGENTE' : pd.to_datetime(df_google['Marca temporal']).dt.strftime('%H:%M:%S'),
    'HORA FIN AGENTE' : pd.to_datetime(df_google['Marca temporal'], format='mixed').dt.strftime('%H:%M:%S'),
    'CLI ID' : '',
    'CLI FECHA TIPIFICACION' : pd.to_datetime(df_google['Marca temporal'], format='mixed').dt.strftime('%d-%m-%Y'),
    'CLI HORA TIPIFICACION' : pd.to_datetime(df_google['Marca temporal'], format='mixed').dt.strftime('%H:%M:%S'),
    'NOMBRE PLAN' : df_google['Nombre Plan'],
    'CIA' : '',
    'TIPO 1' : '', 
    'TIPO 2': '',
    'CUV' : df_google['CUV'],
    'CLI NOMBRE CLIENTE' : df_google['Nombre cliente'],
    'TIPO DE DOCUMENTO' : '1',
    'NUMERO DE DOCUMENTO' : df_google['DNI'],
    'FECHA NACIMIENTO' : '',
    'SEXO' : df_google['Sexo'],
    'TELEFONO 1' : df_google['Teléfono'],
    'TELEFONO 2' : df_google['Teléfono'],
    'CLI EMAIL' : df_google['Correo'],
    'DEPARTAMENTO' : df_google['Departamento'],
    'PROVINCIA' : df_google['Provincia'],
    'DISTRITO': df_google['Distrito'],
    'DIRECCION' : '',
    'NIVEL 1' : df_google['Nivel 1'],
    'NIVEL 2' : df_google['NIVEL_2'],
    'NIVEL 3' : df_google['NIVEL 3'],
    'NIVEL 4' : df_google['NIVEL 4'],
    'MODIFICADO' : '',
    'TMO' : '',
    'CALIDAD' : ''
})

#Concatenar Trama con df_trama_nuevo
df_trama = pd.concat([df_trama, df_trama_google], ignore_index=True)
#df_trama = df_trama_google


#Consolidar Modificaciones de Edson
df_modificaciones_edson = pd.read_excel(os.path.join(Ruta_Edson, 'Ingresar Gestiones CRM.xlsx'), sheet_name="Modificar Tipificacion", header=0,dtype=str).drop(columns=['MOTIVO'])
df_modificaciones_edson['NUMERO DE DOCUMENTO'] = df_modificaciones_edson['NUMERO DE DOCUMENTO'].str.zfill(8)

#Eliminar espacios excesivos en todas las columnas de df_modificaciones_edson
df_modificaciones_edson = df_modificaciones_edson.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Crear una nueva columna de clave única en ambos DataFrames
df_trama['call_id_producto'] = df_trama['CALL ID'] + '_' + df_trama['NOMBRE PLAN']
df_modificaciones_edson['call_id_producto'] = df_modificaciones_edson['CALL ID'] + '_' + df_modificaciones_edson['NOMBRE PLAN']

# Obtener las claves únicas de las modificaciones
call_id_productos_modificados = df_modificaciones_edson['call_id_producto'].unique()

# Eliminar las filas en df_trama que tienen las claves presentes en las modificaciones
df_trama = df_trama[~df_trama['call_id_producto'].isin(call_id_productos_modificados)]

# Eliminar la columna temporal de clave única
df_trama = df_trama.drop(columns=['call_id_producto'])
df_modificaciones_edson = df_modificaciones_edson.drop(columns=['call_id_producto'])

# Añadir las filas modificadas a df_trama
df_trama = pd.concat([df_trama, df_modificaciones_edson], ignore_index=True)

#Mapeo de Nombres

# Crear listas de claves y valores en minúsculas para comparación
keys_lower = {key.lower(): key for key in mapeo_nombres.keys()}
valores_lower = {valor.lower(): valor for valor in mapeo_nombres.values()}

# Función para encontrar la mejor coincidencia primero en las claves, luego en los valores
def obtener_mejor_coincidencia(nombre, keys_lower, valores_lower, mapeo_nombres, cutoff=0.4):
    nombre_lower = nombre.lower()

    # Primero, buscar coincidencia en las claves
    coincidencias_keys = difflib.get_close_matches(nombre_lower, keys_lower.keys(), n=1, cutoff=cutoff)
    if coincidencias_keys:
        # Si encuentra coincidencia en las claves, devuelve el valor correspondiente en el formato correcto
        return mapeo_nombres[keys_lower[coincidencias_keys[0]]]

    # Si no encuentra coincidencia en las claves, buscar en los valores
    coincidencias_values = difflib.get_close_matches(nombre_lower, valores_lower.keys(), n=1, cutoff=cutoff)
    if coincidencias_values:
        return valores_lower[coincidencias_values[0]]  # Devuelve el valor coincidente en el formato correcto

    # Si no encuentra ninguna coincidencia, devuelve el nombre original
    return nombre

df_trama['NOMBRE AGENTE'] = df_trama['NOMBRE AGENTE'].apply(
    lambda x: obtener_mejor_coincidencia(str(x).lower(), keys_lower, valores_lower, mapeo_nombres)
)


# Crear una máscara para las filas donde NIVEL 4 es igual a 'Renuncia'
mask_renuncia = df_trama['NIVEL 4'] == 'Renuncia'

# Crear arrays vacíos para almacenar los motivos de anulación y otras columnas
motivo_anulacion = np.full(len(df_trama), np.nan, dtype=object)
producto_anulacion = np.full(len(df_trama), np.nan, dtype=object)
funcionario_anula = np.full(len(df_trama), np.nan, dtype=object)
fecha_anulacion = np.full(len(df_trama), np.nan, dtype=object)

# Eliminar duplicados y crear diccionarios para búsqueda rápida
df_global_cuv = df_global.drop_duplicates('ID PÓLIZA').set_index('ID PÓLIZA')[['MOTIVO DE ANULACIÓN', 'PRODUCTO', 'FUNCIONARIO ANULA', 'FECHA DE ANULACIÓN']]
dict_cuv_to_info = df_global_cuv.to_dict('index')

df_global_doc = df_global.drop_duplicates('N° DOCUMENTO TITULAR DE CUENTA').set_index('N° DOCUMENTO TITULAR DE CUENTA')[['MOTIVO DE ANULACIÓN', 'PRODUCTO', 'FUNCIONARIO ANULA', 'FECHA DE ANULACIÓN']]
dict_doc_to_info = df_global_doc.to_dict('index')

# Buscar motivos de anulación usando CUV y ID PÓLIZA
for i, row in df_trama.iterrows():
    if mask_renuncia[i]:  # Solo si es Renuncia
        cuv = row['CUV']
        documento = row['NUMERO DE DOCUMENTO']
        # Buscar por CUV
        info = dict_cuv_to_info.get(cuv)
        if info is None:  # Si no se encuentra por CUV, buscar por documento
            info = dict_doc_to_info.get(documento)
        if info is not None:
            motivo_anulacion[i] = info['MOTIVO DE ANULACIÓN']
            producto_anulacion[i] = info['PRODUCTO']
            funcionario_anula[i] = info['FUNCIONARIO ANULA']
            fecha_anulacion[i] = info['FECHA DE ANULACIÓN']

# Agregar las columnas al DataFrame df_trama
df_trama['MOTIVO DE ANULACIÓN'] = motivo_anulacion
df_trama['PRODUCTO'] = producto_anulacion
df_trama['FUNCIONARIO ANULA'] = funcionario_anula
df_trama['FECHA DE ANULACIÓN'] = fecha_anulacion

#Ordenar df_trama y df_global por Fecha
df_trama = df_trama.sort_values(by='FECHA')
df_global = df_global.sort_values(by='FECHA DE ANULACIÓN')

# Crear una máscara para las filas donde NIVEL 4 es igual a 'Retención' o 'Renuncia'
mask_retencion = df_trama['NIVEL 4'].isin(['Retención'])

# Crear un DataFrame con las retenciones y renuncias
df_retenciones_renuncias = df_trama[mask_retencion]

# Identificar bajas en el mismo mes
bajas_prematuras = df_retenciones_renuncias.merge(
    df_global[['ID PÓLIZA', 'FECHA DE ANULACIÓN','MOTIVO DE ANULACIÓN','FUNCIONARIO ANULA']], 
    how='inner', 
    left_on='CUV', 
    right_on='ID PÓLIZA'
)

# Contar bajas prematuras por asesor
bajas_prematuras_por_asesor = bajas_prematuras.groupby('NOMBRE AGENTE').size().reset_index(name='Baja_Prematura')


#Nos quedamos con los registros que son baja y que Motivo anulación es null
df_trama_bajas = df_trama[(df_trama['NIVEL 4'] == 'Renuncia') & (df_trama['MOTIVO DE ANULACIÓN'].isnull())]
df_trama_bajas = df_trama_bajas[['FECHA', 'CALL ID','NOMBRE AGENTE', 'CLI ID', 'NOMBRE PLAN','CLI NOMBRE CLIENTE',  'NUMERO DE DOCUMENTO', 'NIVEL 4', 'CUV','MOTIVO DE ANULACIÓN', 'PRODUCTO', 'FUNCIONARIO ANULA', 'FECHA DE ANULACIÓN']]



# Filtrar solo las renuncias en df_trama
df_renuncias_trama = df_trama[df_trama['NIVEL 4'] == 'Renuncia']

# Primero, filtrar los registros en df_global que no cruzan con df_renuncias_trama por CUV
df_global_no_trama = df_global[~df_global['ID PÓLIZA'].isin(df_renuncias_trama['CUV'])]

# Ahora, filtrar los registros en df_global_no_trama que no cruzan con df_renuncias_trama por N° DOCUMENTO TITULAR DE CUENTA
df_global_no_trama = df_global_no_trama[~df_global_no_trama['N° DOCUMENTO TITULAR DE CUENTA'].isin(df_renuncias_trama['NUMERO DE DOCUMENTO'])]

# Seleccionar las columnas deseadas
df_global_no_trama = df_global_no_trama[['FECHA DE ANULACIÓN', 'ID PÓLIZA', 'FUNCIONARIO ANULA', 'PRODUCTO', 'N° DOCUMENTO TITULAR DE CUENTA']]



# Agrupación por NOMBRE AGENTE y NIVEL 4
df_agrupado = df_trama.groupby(['NOMBRE AGENTE', 'NIVEL 4']).size().unstack(fill_value=0)

# Calcular el total de gestiones solo considerando Renuncia y Retención
df_agrupado['Total_Gestiones'] = df_agrupado[['Renuncia', 'Retención']].sum(axis=1)

# Agregar columna de Baja Prematura sin afectar los cálculos
df_agrupado = df_agrupado.reset_index().merge(bajas_prematuras_por_asesor, on='NOMBRE AGENTE', how='left').fillna(0)

# Calcular el porcentaje de Retención
df_agrupado['% Retención'] = (df_agrupado['Retención'] / df_agrupado['Total_Gestiones']) * 100

# Reemplazar NaN por 0 en el porcentaje de retención
df_agrupado['% Retención'] = df_agrupado['% Retención'].fillna(0)

# Ordenar columnas
df_agrupado = df_agrupado[['NOMBRE AGENTE', 'Consulta', 'Renuncia', 'Retención', 'Baja_Prematura', 'Total_Gestiones', '% Retención']]

# Subir df_trama a la base de datos Tabla Trama_Retenciones 
df_trama.to_sql('Trama_Retenciones', con=engine, if_exists='replace', index=False)

#Eliminar columnas PRODUCTO,FUNCIONARIO ANULA Y FECHA ANULACIÓN 
df_trama = df_trama.drop(columns=['PRODUCTO', 'FUNCIONARIO ANULA', 'FECHA DE ANULACIÓN'])

#Eliminar duplicados en df_trama en base a la columna CLI FECHA TIPIFICACION, CLI HORA TIPIFICACION, NUMERO DE DOCUMENTO, CUV, NIVEL 4 y eliminar el 2do registro
df_trama = df_trama.drop_duplicates(subset=['CLI FECHA TIPIFICACION', 'CLI HORA TIPIFICACION', 'NUMERO DE DOCUMENTO', 'CUV', 'NIVEL 4'], keep='first')


df_trama.to_excel(os.path.join(Ruta,'Retenciones', f'ResultanteRetenciones_{fecha}.xlsx'), index=False)

#Agrupar Nivel 3 y mostrar retenciones y renuncias agrupadas
df_agrupado_nivel3 = df_trama.groupby(['NIVEL 3', 'NIVEL 4']).size().unstack(fill_value=0)


# df_trama_bajas y df_global_no_trama a excel en hojas distintas
with pd.ExcelWriter(os.path.join(Ruta,'Retenciones', f'ErroresRetenciones_{hoy}.xlsx'), engine='openpyxl') as writer:
    df_trama_bajas.to_excel(writer, sheet_name='BajasCRMnoGlobal', index=False)
    df_global_no_trama.to_excel(writer, sheet_name='BajasGlobalNoCRM', index=False)
    df_trama.to_excel(writer, sheet_name='Resultante', index=False)
    df_agrupado.to_excel(writer, sheet_name='AgrupacionPorAgente', index=False)
    df_agrupado_nivel3.to_excel(writer, sheet_name='AgrupacionPorMotivo', index=False)
    bajas_prematuras.to_excel(writer, sheet_name='BajasPrematuras', index=False)



print('Proceso terminado a las ', datetime.now().strftime('%H:%M:%S'))

