import requests
import pandas as pd
import os
import datetime as dt
import json
import locale
import numpy as np
import matplotlib.pyplot as plt
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy import create_engine, text
import difflib
from dateutil.parser import parse
import glob # Para leer archivos de una carpeta
import time
from dateutil.parser import parse
import calendar
import warnings

warnings.filterwarnings("ignore")

# Configurar locale para español (Perú)
locale.setlocale(locale.LC_TIME, 'es_PE.UTF-8')

# Variables PERIODO ABRIL 2024
fecha = '202409'

# Crear la fecha de inicio del mes concatenando con '01'
fecha_inicio = f"{fecha}01"
fecha_inicio_dt = pd.to_datetime(fecha_inicio, format='%Y%m%d')

usuario = 'azaer'

# Calcular el último día del mes
anio = int(fecha[:4])
Mes = int(fecha[4:6])
ultimo_dia = calendar.monthrange(anio, Mes)[1]

# Crear la fecha de fin del mes
fecha_fin = f"{fecha}{ultimo_dia}"
fecha_fin_dt = pd.to_datetime(fecha_fin, format='%Y%m%d')

print(f"Fecha de inicio: {fecha_inicio_dt}")
print(f"Fecha de fin: {fecha_fin_dt}")

# Convertir las fechas a cadenas en formato YYYY-MM-DD para usar en SQL
fecha_inicio_str = fecha_inicio_dt.strftime('%Y-%m-%d')
fecha_fin_str = fecha_fin_dt.strftime('%Y-%m-%d')

hoy = dt.datetime.now().strftime('%d%m%Y')

# Mes a partir del fecha
mes = dt.datetime.strptime(fecha, '%Y%m').strftime('%B').upper()
Ruta = rf'C:\Users\{usuario}\Documents\Diego\Reportes'
Documentos = rf'C:\Users\{usuario}\Documents'
Descargas = rf'C:\Users\{usuario}\Downloads'

# procesamiento de CSV (LLamadas)


# Leer archivo de llamadas_{mes} más reciente de la carpeta de descargas
archivo = max(glob.glob(f'{Documentos}\\llamadas_{mes}.csv'), key=os.path.getctime)
df_llamadas = pd.read_csv(archivo, sep=';',dtype = str)

#Extracción de Tipificación desde excel
df_tipificaciones = pd.read_excel(f'{Documentos}\\RESULTANTEVENTASPENDIENTE.xlsx', sheet_name='Hoja1', dtype=str)

#Consolidar df_llamadas con df_tipificaciones
df_llamadas =pd.concat([df_llamadas, df_tipificaciones], ignore_index=True)

# Limpiar la columna 'Agent Name'
df_llamadas['Agent Name'] = df_llamadas['Agent Name'].fillna('').apply(lambda x: x.split(',')[0] if ',' in x else x)

#Reemplazar valores vacios de CallCode por 'NO CONTESTA
df_llamadas['Callcode'] = df_llamadas['Callcode'].fillna('NO CONTESTA / OCUPADO')
df_llamadas['Agent Name'] = df_llamadas['Agent Name'].replace('','Resultado Marcador Predictivo')


#Filtrar registros innecesarios
df_llamadas = df_llamadas[df_llamadas['Callcode'] != 'Default Callcode']
df_llamadas = df_llamadas[~df_llamadas['Queue name'].isin(['Outbound_Retenciones_Ripley', 'Regrabacion', 'Inbound_Retenciones_Ripley'])]

#Eliminar registros especificos que no son venta
NO_VENTA = ['472479','515497','441234' ,'524952','418805','450129','436163','461054','663162', #Junio
           '675934', '700552', '704594','779241','774593','768197','785202','835875','835752', #Julio
           '912513', '970101', # Agosto
           '1057817','1057875']   #Septiembre
df_llamadas = df_llamadas[~df_llamadas['CallTraceID'].isin(NO_VENTA)]

# donde TraceID es igual a 745788 Cambiar Tipificacion a VENTA
df_llamadas.loc[df_llamadas['CallTraceID'].isin(['745788', '864052','879455' ,  #Julio
                                                 '927008','1013629']), 'Callcode'] = 'VENTA' #Agosto

# Definir el mapeo de tipificaciones de nivel 2 a nivel 1
tipificaciones_nivel1 = {
    'ND LO LLAMARON MAS DE UNA VEZ': 'EFECTIVO',
    'NO DESEA - YA LE OFRECIERON': 'EFECTIVO',
    'ND POR COSTO': 'EFECTIVO',
    'ND NO TIENE TARJETA': 'EFECTIVO',
    'ND COYUNTURAL': 'EFECTIVO',
    'ND NO CONFORME RIPLEY': 'EFECTIVO',
    'REVALIDACION': 'EFECTIVO',
    'VOLVER A LLAMAR': 'EFECTIVO',
    'CLIENTE CORTO LLAMADA CON INFO': 'EFECTIVO',
    'ND NO BRINDA MOTIVO': 'EFECTIVO',
    'ND NO CONTRATA NADA POR TELF.': 'EFECTIVO',
    'VENTA': 'EFECTIVO',
    'GRABADORA': 'NO CONTACTO',
    'BUZON DE VOZ': 'NO CONTACTO',
    'NO CONTESTA / OCUPADO': 'NO CONTACTO',
    'FUERA DE SERVICIO / SUSPENDIDO': 'NO CONTACTO',
    'TITULAR INUBICABLE': 'NO CONTACTO',
    'CONTACTO CON TERCERO': 'NO EFECTIVO',
    'CORTA LLAMADA SIN INFO': 'NO EFECTIVO',
    'NUMERO EQUIVOCADO': 'NO EFECTIVO'
}

# Mapear las tipificaciones de nivel 2 a nivel 1
df_llamadas['Tipificacion_Nivel1'] = df_llamadas['Callcode'].map(tipificaciones_nivel1)

# Definir prioridades
prioridades = {
    'EFECTIVO': 2,
    'NO EFECTIVO': 3,
    'NO CONTACTO': 4
}

df_llamadas['Prioridad'] = np.where(df_llamadas['Callcode'] == 'VENTA', 1, df_llamadas['Tipificacion_Nivel1'].map(prioridades).fillna(5))


#Formatear Columna Fecha IncomingCallTime a formato fecha (2024-06-13 08:50:41 a 2 columnas de Fecha y hora)
df_llamadas['Fecha'] = pd.to_datetime(df_llamadas['IncomingCallTime'], format='mixed').dt.date
df_llamadas['Hora'] = pd.to_datetime(df_llamadas['IncomingCallTime'], format='mixed').dt.time

#Renombrar columna DNIS por Numero
df_llamadas.rename(columns={'DNIS': 'Numero'}, inplace=True)

# Ordenar por DNI, Prioridad y Fecha
df_llamadas = df_llamadas.sort_values(by=['Numero', 'Prioridad', 'Fecha','Hora'], ascending=[True, True, True, False])

# Seleccionar Columnas relevantes
df_llamadas = df_llamadas[['Call Type','Numero','Queue name', 'Fecha', 'Hora', 'Tipificacion_Nivel1', 'Callcode','Client talk time', 'Prioridad','CallTraceID','Agent Name']]


# Definir los parámetros de conexión
server = r'NombreBDD'
database = 'database'
schema = 'dbo'
username = 'user'
password = 'password'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'

# Crear el motor de conexión
engine = create_engine(f'mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')

# Crear la conexión
connection = pyodbc.connect(connection_string)
# Crear un cursor
cursor = connection.cursor()

# Realizar Select a la tabla de clientes para Obtener DNI, Campaña y captación

query = f"""
select id,dni,ceL1,ceL2,campana,captación,calL_ID from Reportes_Chubb..Clientes where aniomes = '{fecha}'
"""

df_clientes = pd.read_sql(query, connection,dtype=str)

# Inicializar las columnas nuevas con NaN
df_llamadas['DNI_CLIENTE'] = np.nan
df_llamadas['campana'] = np.nan
df_llamadas['captación'] = np.nan

# Eliminar duplicados en 'ceL1' y 'ceL2' para cada columna relevante
df_clientes_ceL1 = df_clientes.drop_duplicates(subset=['ceL1'], keep='first')
df_clientes_ceL2 = df_clientes.drop_duplicates(subset=['ceL2'], keep='first')

# Ahora realizar el mapeo sin duplicados
dni_ceL1 = df_llamadas['Numero'].map(df_clientes_ceL1.set_index('ceL1')['dni'])
dni_ceL2 = df_llamadas['Numero'].map(df_clientes_ceL2.set_index('ceL2')['dni'])

campana_ceL1 = df_llamadas['Numero'].map(df_clientes_ceL1.set_index('ceL1')['campana'])
campana_ceL2 = df_llamadas['Numero'].map(df_clientes_ceL2.set_index('ceL2')['campana'])

captacion_ceL1 = df_llamadas['Numero'].map(df_clientes_ceL1.set_index('ceL1')['captación'])
captacion_ceL2 = df_llamadas['Numero'].map(df_clientes_ceL2.set_index('ceL2')['captación'])

# Utilizar np.where para asignar valores dependiendo de las coincidencias
df_llamadas['DNI_CLIENTE'] = np.where(dni_ceL1.notna(), dni_ceL1, dni_ceL2)
df_llamadas['campana'] = np.where(campana_ceL1.notna(), campana_ceL1, campana_ceL2)
df_llamadas['captación'] = np.where(captacion_ceL1.notna(), captacion_ceL1, captacion_ceL2)

# Convertir 'Client talk time' a segundos
df_llamadas['Client talk time'] = pd.to_timedelta(df_llamadas['Client talk time']).dt.total_seconds()

#Crear columna de TMO_Segundos
df_llamadas['TMO_Segundos'] = df_llamadas['Client talk time']

# Calcular TMO_Total y Intentos_Total por DNI_CLIENTE
df_totales = df_llamadas.groupby('DNI_CLIENTE').agg(
    TMO_Total=('Client talk time', 'sum'),
    Intentos_Total=('DNI_CLIENTE', 'size')
).reset_index()

# Convertir TMO_Total de segundos a hh:mm:ss
df_totales['TMO_Total'] = pd.to_datetime(df_totales['TMO_Total'], unit='s').dt.strftime('%H:%M:%S')


# Calcular TMO_Total_Dia y Intentos_Dia por DNI_CLIENTE y Fecha
df_totales_dia = df_llamadas.groupby(['DNI_CLIENTE', 'Fecha']).agg(
    TMO_Total_Dia=('Client talk time', 'sum'),
    Intentos_Dia=('DNI_CLIENTE', 'size')
).reset_index()

# Convertir TMO_Total_Dia de segundos 
df_totales_dia['TMO_Total_Dia'] = pd.to_datetime(df_totales_dia['TMO_Total_Dia'], unit='s').dt.strftime('%H:%M:%S')

# Unir los resultados totales por DNI
df_llamadas = df_llamadas.merge(df_totales[['DNI_CLIENTE', 'TMO_Total', 'Intentos_Total']], on='DNI_CLIENTE', how='left')

# Unir los resultados totales por DNI y Fecha
df_llamadas = df_llamadas.merge(df_totales_dia[['DNI_CLIENTE', 'Fecha', 'TMO_Total_Dia', 'Intentos_Dia']], on=['DNI_CLIENTE', 'Fecha'], how='left')

# Asegúrate de que los datos estén ordenados por DNI_CLIENTE, Fecha, y Hora
df_llamadas = df_llamadas.sort_values(by=['DNI_CLIENTE', 'Fecha', 'Hora'])

# Crear una columna 'FechaHora' combinando 'Fecha' y 'Hora'
df_llamadas['FechaHora'] = pd.to_datetime(df_llamadas['Fecha'].astype(str) + ' ' + df_llamadas['Hora'].astype(str))

# Calcular la diferencia de tiempo en minutos entre cada intento, agrupado por 'DNI_CLIENTE'
df_llamadas['Diferencia_Tiempo'] = df_llamadas.groupby('DNI_CLIENTE')['FechaHora'].diff().dt.total_seconds() / 60

# Crear una columna para marcar el inicio de un nuevo trámite
df_llamadas['Nuevo_Tramite'] = (df_llamadas['Diferencia_Tiempo'].isna()) | (df_llamadas['Diferencia_Tiempo'] > 60)

# Asignar un ID de trámite incremental para cada grupo de trámites
df_llamadas['ID_Tramite'] = df_llamadas.groupby('DNI_CLIENTE')['Nuevo_Tramite'].cumsum()

# Contar el número de trámites por DNI_CLIENTE
df_tramites = df_llamadas.groupby('DNI_CLIENTE').agg(
    Intentos_Total=('DNI_CLIENTE', 'size'),
    Tramites_Total=('ID_Tramite', 'nunique')
).reset_index()

# Unir el cálculo de trámites al DataFrame original
df_llamadas = df_llamadas.merge(df_tramites[['DNI_CLIENTE', 'Tramites_Total']], on='DNI_CLIENTE', how='left')

# Eliminar las columnas: 'Diferencia_Tiempo', 'Nuevo_Tramite' y 2 otras columnas
df_llamadas = df_llamadas.drop(columns=['Diferencia_Tiempo', 'Nuevo_Tramite', 'ID_Tramite', 'FechaHora'])

#Exportar a excel todas las filas donde dni es nulo
df_llamadas[df_llamadas['DNI_CLIENTE'].isnull()].to_excel(f'{Ruta}\\Llamadas_sin_dni_{mes}.xlsx', index=False)

#Tomando en cuenta la conexión y el cursor, eliminemos los registros de Gestion_Mediatel que coincidan con el rango de inicio y termino mes

# Ejecutar la consulta DELETE con parámetros
delete_query = "DELETE FROM dbo.Gestion_Mediatel WHERE Fecha >= ? AND Fecha <= ?"
cursor.execute(delete_query, (fecha_inicio_str, fecha_fin_str))

# Confirmar los cambios
connection.commit()

# Verificar el número de filas afectadas
print(f'Filas eliminadas: {cursor.rowcount}')

# Insertamos los registros de llamadas en la tabla Gestion_Mediatel

try:
    # Crear una copia del DataFrame y renombrar columnas solo dentro del bloque try
    df_llamadas_temp = df_llamadas.copy()

       # Mapeo de nombres de columnas del DataFrame a los nombres de columnas en la tabla SQL
    mapeo_columnas = {
        'campana': 'Campaña',  # Cambiar 'campana' a 'Campaña'
        'Intentos_Total' : 'Cantidad_llamadas',  # Cambiar 'Intentos_Total' a 'Cantidad_llamadas'
        'captación': 'Captacion',  # Cambiar 'captación' a 'Captacion'
        'TMO_Total': 'tmo',  # Cambiar 'TMO_Total' a 'tmo'
    }

    # Renombrar las columnas en la copia del DataFrame
    df_llamadas_temp.rename(columns=mapeo_columnas, inplace=True)

    # Subir los nuevos datos a la tabla de SQL Server sin reemplazar toda la tabla
    df_llamadas_temp.to_sql(name='Gestion_Mediatel', con=engine, if_exists='append', index=False)
    print('Nuevos datos subidos a la base de datos')
except Exception as e:
    print(f'Error al subir los nuevos datos: {e}')

# Cerrar el cursor solamente
cursor.close()


# Proceso de Subida información de Adicionales,lectura de API adicionales y segmentador

# Función para convertir fechas al formato YYYYMMDD
def convertir_fecha(fecha):
    try:
        # Intentar convertir con el formato específico 'dd/mm/yyyy'
        fecha_dt = pd.to_datetime(fecha, format='%d/%m/%Y', errors='coerce', dayfirst=True)
        if not pd.isna(fecha_dt):
            return fecha_dt.strftime('%Y%m%d')
    except (ValueError, TypeError):
        pass

    try:
        # Intentar convertir con el formato 'YYYY-MM-DD'
        fecha_dt = pd.to_datetime(fecha, format='%Y-%m-%d', errors='coerce')
        if not pd.isna(fecha_dt):
            return fecha_dt.strftime('%Y%m%d')
    except (ValueError, TypeError):
        pass

    try:
        # Intentar convertir con el formato 'YYYY-MM-DD'
        fecha_dt = pd.to_datetime(fecha, format='%d-%m-%Y', errors='coerce')
        if not pd.isna(fecha_dt):
            return fecha_dt.strftime('%Y%m%d')
    except (ValueError, TypeError):
        pass

    try:
        # Intentar conversión de formato '26 de diciembre de 1988'
        fecha_dt = pd.to_datetime(fecha, format='%d de %B de %Y', errors='coerce')
        if not pd.isna(fecha_dt):
            return fecha_dt.strftime('%Y%m%d')
    except (ValueError, TypeError):
        pass

    # Si no coincide con ningún formato, devuelve la fecha original
    return fecha

def leer_excel_con_tipos_especificos(url, nombre_hoja, columna_fecha):
    # Leer todas las columnas como texto
    dtype_dict = {col: str for col in pd.read_excel(url, sheet_name=nombre_hoja, nrows=0).columns}
    
    # Especificar que la columna de fecha debe ser parseada como fecha
    dtype_dict.pop(columna_fecha, None)
    
    # Leer el archivo Excel con las especificaciones
    df = pd.read_excel(url, 
                       sheet_name=nombre_hoja, 
                       dtype=dtype_dict, 
                       parse_dates=[columna_fecha])
    
    return df


url2 = f'https://app.soluziona.pe/API_QA/Peru/CRM/api/Excel_CRM/CRM/Reporte/Excel/Cliente/Adicionales/{fecha}'



try:
    # Realiza las solicitudes a ambas APIs
    response2 = list(requests.get(url, timeout=10) for url in url2)
    df_adicionales = pd.DataFrame(response2.json())

except (requests.exceptions.RequestException, ValueError) as e:
    # Si hay un error (como que la URL no responda o error de conexión), crea un DataFrame con datos ficticios
    print(f"Error al intentar acceder a la URL o procesar datos: {e}")
    df_adicionales = pd.DataFrame([{
        'clI_ID': '0',
        'feC_VENTA': f'{fecha}' + '01',
        'tipO_ASEG': 'OTRA RELACIÓN',
        "feC_NACIMIENTO": "1983-09-09",
        'parentescO_ID': '0',
        "tipO_DOC_ID": "D.N.I.",
        'nrO_DOC': '00000000',
        'apE_PATERNO': '0',
        'apE_MATERNO': '0',
        'clI_ANOMBRE1': '0',
        'clI_ANOMBRE2': '0',
        'sexo': '0',
        'estadO_CIVIL': '0',
        'email': '0',
        'telefonO_MOVIL': '0',
        'campana': 'SONRIE_SEGURO'
    }])



df_adicionales['feC_VENTA'] = df_adicionales['feC_VENTA'].apply(convertir_fecha)
df_adicionales['feC_NACIMIENTO'] = pd.to_datetime(df_adicionales['feC_NACIMIENTO'], format='mixed').dt.strftime('%Y%m%d')

# Crear la conexión
connection = pyodbc.connect(connection_string)
# Crear un cursor
cursor = connection.cursor()

delete_query_adicionales = f"DELETE FROM Reportes_Chubb..adicionales WHERE feC_VENTA like '%{fecha}%'"
cursor.execute(delete_query_adicionales)
adicionales_eliminados = cursor.rowcount

# Confirmar los cambios
connection.commit()

print(f'Filas eliminadas en Adicionales: {adicionales_eliminados}')

print(delete_query_adicionales)


archivos_segmentados = [
    archivo for archivo in glob.glob(os.path.join(Descargas, "*SEGMENTADOR*.xlsx"))
    if not os.path.basename(archivo).startswith('~$')
]

# Seleccionar el archivo más reciente
archivo_mas_reciente = max(archivos_segmentados, key=os.path.getmtime, default=None)
# Mostrar la fecha de modificacion del archivo
fecha_modificacion = dt.datetime.fromtimestamp(os.path.getmtime(archivo_mas_reciente)).strftime('%Y-%m-%d %H:%M:%S')
print(f'Archivo más reciente: {archivo_mas_reciente} actualizado al {fecha_modificacion}')

mes = dt.datetime.strptime(fecha, '%Y%m').strftime('%B').upper()

segmentador_google = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQE03PGEUQUyYQkqyn5_ZJf7fUTlwgaWpHY8hVVunUbw6Apl12oQ5s2yJX3oVZPSg/pub?output=xlsx'
mes = dt.datetime.strptime(fecha, '%Y%m').strftime('%B').upper()
segmentador = leer_excel_con_tipos_especificos(segmentador_google, mes, 'Fecha Venta')


# Verificar las conversiones
print(segmentador['Fecha Venta'].head())

segmentador['Fecha Venta'] = pd.to_datetime(segmentador['Fecha Venta']).dt.strftime('%Y%m%d')

#segmentador = pd.read_excel(archivo_mas_reciente, sheet_name= f'{mes}', dtype = str)
segmentador['N Documento'] = segmentador['N Documento'].str[-8:].str.zfill(8)
segmentador = segmentador[['SAP TMK', 'Telefono', 'CALL ID', 'ID', 'Fecha Venta',  'Agente',
       'Plan', 'N Documento', 'Nombres', 'Paterno', 'Materno',
       'Fecha de Nacimiento' , 'Dirección',
       'Departamento', 'Provincia', 'Distrito', 'Email',  'Descripción Producto', 'Prima', 'Fecha Grabación',  'Grabada' ]]
    
#Formatear Columna Fecha Venta a formato fecha YYYYMMDD desde el Formato DD-MM-YYYY



# Intentar convertir las fechas asegurando que se lee el formato correcto 'dd/mm/yyyy'
#segmentador['Fecha Venta'] = segmentador['Fecha Venta'].apply(convertir_fecha)


segmentador['Fecha de Nacimiento'] = segmentador['Fecha de Nacimiento'].apply(convertir_fecha)

# Verificar las conversiones
print(segmentador['Fecha Venta'].head())


#Agregar columna de VentasTotales
segmentador['VentasTotales'] = segmentador['Plan'].map(
    lambda x: 2 if pd.notnull(x) and '1' in x else
              3 if pd.notnull(x) and '2' in x else
              4 if pd.notnull(x) and '3' in x else
              1 if pd.notnull(x) and 'titular' in x.lower() and 'adicional' not in x.lower() else
              0
)

# Actualizar la tabla de Adicionales con los datos de segmentador siempre y cuando [N Documento] no se encuentre en la tabla de Adicionales y Plan = 'Adicional'


#Quedarme con los dni que estén en segmentador 
df_clientes = df_clientes[df_clientes['dni'].isin(segmentador['N Documento'])]

'''
#Actualizar la columna de ID en segmentador con ID de la tabla de clientes usando el CALL ID como llave
segmentador = segmentador.drop(columns = ['ID'])
segmentador = segmentador.merge(df_clientes[['id','calL_ID']], left_on='CALL ID', right_on='calL_ID', how='left').drop(columns = ['calL_ID'])


# Paso 2: Identificar las filas que no se cruzaron correctamente (ID es nulo)
sin_id = segmentador[segmentador['id'].isnull()]

# Paso 3: Obtener el CALL ID correcto usando el DNI del titular
if not sin_id.empty:
    # Obtener el CALL ID correcto del titular
    titular = df_clientes[['dni', 'calL_ID']].drop_duplicates(subset=['dni'])
    sin_id = sin_id.merge(titular, left_on='N Documento', right_on='dni', how='left').drop(columns=['dni'])
    
    # Crear un diccionario de mapeo de CALL ID erróneo a CALL ID correcto
    call_id_mapping = dict(zip(sin_id['CALL ID'], sin_id['calL_ID']))

    
    # Actualizar el CALL ID en el segmentador usando replace
    segmentador['CALL ID'] = segmentador['CALL ID'].replace(call_id_mapping)


    # Repetir el cruce inicial con los CALL ID actualizados
    segmentador = segmentador.drop(columns=['id'])
    segmentador = segmentador.merge(df_clientes[['id', 'calL_ID']], left_on='CALL ID', right_on='calL_ID', how='left').drop(columns=['calL_ID'])

# Validar y limpiar los IDs actualizados
segmentador['ID'] = segmentador['id'].fillna('ID NO ENCONTRADO')
segmentador = segmentador.drop(columns=['id'])
'''
#Obtener ID de df_clientes si ID es nulo
segmentador = segmentador.merge(df_clientes[['id','dni']], left_on='N Documento', right_on='dni', how='left').drop(columns = ['dni'])

# Reemplazar todos los valores de ID segmentador por el nuevo ID cliente donde ID sea nulo
segmentador['ID'] = segmentador['id'].fillna(segmentador['ID'])

#Eliminar columnas innecesarias
segmentador = segmentador.drop(columns=['id'])

#Reemplazar todos los valores de ID segmentador por el nuevo ID cliente

segmentador2 = segmentador[segmentador['Plan'] == 'Adicional']

df_adicionales_2 = pd.DataFrame({
    'clI_ID' : segmentador2['ID'],
    'feC_VENTA' : segmentador2['Fecha Venta'],
    'tipO_ASEG' : 'OTRA RELACIÓN',
    'feC_NACIMIENTO' : segmentador2['Fecha de Nacimiento'],
    'nrO_DOC' : segmentador2['N Documento'],
    'apE_PATERNO' : segmentador2['Paterno'],
    'apE_MATERNO' : segmentador2['Materno'],
    'clI_ANOMBRE1' : segmentador2['Nombres'].str.split(' ').str[0],
    'clI_ANOMBRE2' : segmentador2['Nombres'].str.split(' ').str[1],
    'sexo' : '',
    'estadO_CIVIL' : '',
    'email' : segmentador2['Email'],
    'telefonO_MOVIL' : segmentador2['Telefono'],    
    })


# Primer filtro: Filtrar df_adicionales_2 para que solo contenga filas donde el número de documento no esté en df_adicionales
df_adicionales_2 = df_adicionales_2[~df_adicionales_2['nrO_DOC'].isin(df_adicionales['nrO_DOC'])]

# Concatenar los DataFrames de Adicionales
df_adicionales = pd.concat([df_adicionales, df_adicionales_2], ignore_index=True)

# Insertar los nuevos datos
try:
    df_adicionales.to_sql(name='adicionales', con=engine, if_exists='append', index=False)
    print('Nuevos datos subidos a la base de datos')
except Exception as e:
    print(f'Error al subir los nuevos datos: {e}')

#Comienza insersión de Datos en segmentador

#Eliminar registros de segmentador que coincidan con el rango de inicio y termino mes

delete_query_segmentador = f"DELETE FROM Reportes_Chubb..segmentador WHERE [Fecha Venta] like '%{fecha}%'"
cursor.execute(delete_query_segmentador)

print(delete_query_segmentador)

# Confirmar los cambios
connection.commit()

print(f'Filas eliminadas en Segmentador: {cursor.rowcount}')

# Insertar los nuevos datos
try:
    # Subir los nuevos datos a la tabla de SQL Server sin reemplazar toda la tabla
    segmentador.to_sql(name='Segmentador', con=engine, if_exists='append', index=False)
    print('Nuevos datos subidos a la base de datos')
except Exception as e:
    print(f'Error al subir los nuevos datos: {e}')

# Ejecutar el procedimiento almacenado desde Python
sp_query = f"EXEC Reportes_Chubb..ObtenerTipificaciones '{fecha}'"
cursor.execute(sp_query)
connection.commit()

# Cerrar el cursor y la conexión
cursor.close()
connection.close()


print('Proceso terminado a las ', dt.datetime.now().strftime('%H:%M:%S'))
print(segmentador['Grabada'].unique())
