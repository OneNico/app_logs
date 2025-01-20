# src/funcion/data_processing.py

import pandas as pd
import orjson  # Parser JSON más rápido
import io

# Definición de los tipos de datos para las columnas
COL_DTYPES = {
    # -------------------------------------------------------------------------
    # Columna de Fecha/Hora principal (la que parseamos manualmente al leer)
    "Datetime": "datetime64[ns]",

    # -------------------------------------------------------------------------
    # source.*
    "source.Redundancy Group": "Int64",
    "source.App Type Id": "Int64",
    "source.Server Id": "Int64",
    "source.Repeater Id": "Int64",
    "source.Device Site Id": "Int64",
    "source.Repeater Slots": "string",  # "first_channel", "second_channel" etc.

    # -------------------------------------------------------------------------
    # descriptor.*
    "descriptor.Protocol": "Int64",      # aparece como 0
    "descriptor.Opcode": "string",       # "ATIACallStatusPdu", etc.
    "descriptor.Source Unique Id": "Int64",
    "descriptor.Destination Unique Id": "Int64",
    "descriptor.Sequence Number": "Int64",
    "descriptor.Timestamp": "Int64",     # o "float64". Si quisieras fecha, tendrías que convertir
    "descriptor.Fragment": "Int64",
    "descriptor.Version": "Int64",
    "descriptor.Role Info": "string",

    # -------------------------------------------------------------------------
    # payload.*
    "payload.Timestamp": "Int64",  # igual que descriptor.Timestamp
    "payload.Csn": "Int64",
    "payload.Src": "Int64",
    "payload.Tgt": "Int64",
    "payload.Site all call site id": "Int64",
    "payload.CallType": "string",
    "payload.Action": "string",
    "payload.Info": "string",
    "payload.Value": "Int64",
    "payload.Originating Site Id": "Int64",
    "payload.RSSI": "string",  # incluye " dbm", "Not available", etc.
    "payload.Participating sites": "object",  # lista/dict
    "payload.Logical Channel Number": "Int64",
    "payload.Talkgroup Subscription List": "object",  # lista/dict
    "payload.Radio Type": "string",
    "payload.Phone Info": "string",
    "payload.Preempt Priority": "string",
    "payload.TG Alias": "string",
    "payload.TG Id": "string",
    "payload.State": "string"
}

def procesar_archivo(archivo):
    """
    Procesa un solo archivo .log y retorna una lista de registros.

    Args:
        archivo (UploadedFile): Archivo subido por el usuario.

    Returns:
        list: Lista de registros procesados.
    """
    registros = []
    try:
        # Resetear el buffer al inicio
        archivo.seek(0)
        # Envolver el archivo con TextIOWrapper para leer como texto
        text_stream = io.TextIOWrapper(archivo, encoding="utf-8")

        while True:
            fecha_hora = text_stream.readline()
            if not fecha_hora:
                break  # Fin del archivo
            json_str = text_stream.readline()
            if not json_str:
                # Posible final del archivo sin JSON
                break

            try:
                # Parsear el JSON usando orjson para mayor velocidad
                registro = orjson.loads(json_str)
                # Agregar la fecha y hora al registro
                registro['Datetime'] = pd.to_datetime(
                    fecha_hora.strip(),
                    format="%d-%m-%Y %H:%M:%S",
                    errors='coerce'
                )
                registros.append(registro)
            except orjson.JSONDecodeError:
                # Si hay un error al parsear, simplemente omite este registro
                continue

    except Exception as e:
        # Si ocurre un error al leer el archivo, retorna una lista vacía
        return []

    return registros

def cargar_logs(archivos):
    """
    Carga múltiples archivos .log y los concatena en un único DataFrame de manera eficiente.

    Args:
        archivos (list): Lista de objetos de archivos subidos por el usuario.

    Returns:
        pd.DataFrame: DataFrame combinado con todos los datos cargados.
    """
    datos = []

    for archivo in archivos:
        registros = procesar_archivo(archivo)
        datos.extend(registros)

    if datos:
        try:
            # Normalizar los datos anidados en JSON
            df = pd.json_normalize(datos)
            # Aplicar los tipos de datos definidos en COL_DTYPES
            df = df.astype(COL_DTYPES, errors='ignore')  # errors='ignore' para evitar errores en columnas no presentes
            return df
        except Exception as e:
            # En caso de error al normalizar, retorna un DataFrame vacío
            return pd.DataFrame()
    else:
        # Si no hay datos, retorna un DataFrame vacío
        return pd.DataFrame()
