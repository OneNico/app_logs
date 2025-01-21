import vaex
import orjson  # Parser JSON más rápido
import io

# Definición de los tipos de datos para las columnas
COL_DTYPES = {
    "Datetime": "datetime64[ns]",
    "source.Redundancy Group": "int64",
    "source.App Type Id": "int64",
    "source.Server Id": "int64",
    "source.Repeater Id": "int64",
    "source.Device Site Id": "int64",
    "source.Repeater Slots": "string",
    "descriptor.Protocol": "int64",
    "descriptor.Opcode": "string",
    "descriptor.Source Unique Id": "int64",
    "descriptor.Destination Unique Id": "int64",
    "descriptor.Sequence Number": "int64",
    "descriptor.Timestamp": "int64",
    "descriptor.Fragment": "int64",
    "descriptor.Version": "int64",
    "descriptor.Role Info": "string",
    "payload.Timestamp": "int64",
    "payload.Csn": "int64",
    "payload.Src": "int64",
    "payload.Tgt": "int64",
    "payload.Site all call site id": "int64",
    "payload.CallType": "string",
    "payload.Action": "string",
    "payload.Info": "string",
    "payload.Value": "int64",
    "payload.Originating Site Id": "int64",
    "payload.RSSI": "string",
    "payload.Participating sites": "string",
    "payload.Logical Channel Number": "int64",
    "payload.Talkgroup Subscription List": "string",
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
        archivo.seek(0)
        text_stream = io.TextIOWrapper(archivo, encoding="utf-8")

        while True:
            fecha_hora = text_stream.readline()
            if not fecha_hora:
                break
            json_str = text_stream.readline()
            if not json_str:
                break

            try:
                registro = orjson.loads(json_str)
                registro['Datetime'] = fecha_hora.strip()
                registros.append(registro)
            except orjson.JSONDecodeError:
                continue

    except Exception:
        return []

    return registros

def cargar_logs(archivos):
    """
    Carga múltiples archivos .log y los concatena en un único DataFrame de manera eficiente utilizando Vaex.

    Args:
        archivos (list): Lista de objetos de archivos subidos por el usuario.

    Returns:
        vaex.DataFrame: DataFrame combinado con todos los datos cargados.
    """
    datos = []

    for archivo in archivos:
        registros = procesar_archivo(archivo)
        datos.extend(registros)

    if datos:
        try:
            # Normalizar los datos anidados en JSON
            df_pandas = pd.json_normalize(datos)
            
            # Convertir a un DataFrame de Vaex
            df_vaex = vaex.from_pandas(df_pandas, copy_index=False)

            # Aplicar tipos de datos definidos
            for col, dtype in COL_DTYPES.items():
                if col in df_vaex.columns:
                    try:
                        df_vaex[col] = df_vaex[col].astype(dtype)
                    except Exception:
                        continue

            return df_vaex
        except Exception:
            return vaex.from_arrays()  # Retorna un DataFrame Vaex vacío
    else:
        return vaex.from_arrays()  # Retorna un DataFrame Vaex vacío
