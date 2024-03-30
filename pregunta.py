"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd

def ingest_data():
    ruta_archivo = "clusters_report.txt"

    df = pd.read_fwf(
        ruta_archivo,
        colspecs="infer",
        widths=[9,16,16,80],
        header = None,
        names = ["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave", "principales_palabras_clave"], 
        converters = {"porcentaje_de_palabras_clave": lambda x: x.rstrip(" %").replace(",", ".")}
    ).drop(index={0,1,2}).ffill()

    df["cluster"] = pd.to_numeric(df["cluster"])
    df['cantidad_de_palabras_clave'] = pd.to_numeric(df['cantidad_de_palabras_clave'])
    df['porcentaje_de_palabras_clave'] = pd.to_numeric(df['porcentaje_de_palabras_clave'])

    concatenated_column = df.groupby(['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave'])['principales_palabras_clave'].apply(lambda x: ' '.join(x)).reset_index()

    concatenated_column['principales_palabras_clave'] = concatenated_column['principales_palabras_clave'].replace(r'\s+', ' ', regex=True)
    concatenated_column["principales_palabras_clave"] = concatenated_column["principales_palabras_clave"].apply(lambda x : x.replace(".", ""))

    return concatenated_column

