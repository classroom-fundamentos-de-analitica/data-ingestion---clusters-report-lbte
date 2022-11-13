"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import numpy as np

def ingest_data():
  df = pd.read_fwf('./clusters_report.txt', colspecs="infer", widths=[7, 11, 13, 300])
  with open('./clusters_report.txt', 'r') as cluster_file:
    splitted_lines = [line.split(", ") for line in cluster_file.readlines()]

    cols11 = splitted_lines[0][0].split()
    cols12 = splitted_lines[1][0].split()
    col1 = cols11[0]
    col2 = ' '.join(cols11[1:3])
    col3 = ' '.join(cols11[3:5])
    col4 = ' '.join(cols11[5:]) 
    col2 += ' ' + ' '.join(cols12[0:2])
    col3 += ' ' + ' '.join(cols12[2:])

    columnas = [col1, col2, col3, col4]
  df.columns = columnas
  df = df.iloc[2:, :].reset_index()
  df.drop("index", inplace=True, axis='columns')

  df.iloc[:,:-1].fillna(method = "ffill", inplace = True)
  df[df.columns[0]] = df[df.columns[0]].astype(int)
  non_last = df.columns[:-1]
  df_corrected = df.groupby(df.columns[0])["Principales palabras clave"].apply(lambda x: ' '.join(x))
  df_corrected[df.columns[0]] = np.arange(1, df.iloc[:, 0].max())
  df = df.merge(df_corrected, on=df.columns[0], how="left").drop_duplicates(non_last)
  df.drop("Principales palabras clave_x", inplace=True, axis=1)
  df.rename(columns={'Principales palabras clave_y':"Principales palabras clave"}, inplace=True)
  cols = list(map(lambda x: x.lower().replace(" ", "_"), df.columns))
  cols = {original:renamed for original, renamed in zip(df.columns, cols)}
  df.rename(columns = cols, inplace = True)
  df.porcentaje_de_palabras_clave = df.porcentaje_de_palabras_clave.str.replace(" %", "")
  df.porcentaje_de_palabras_clave = df.porcentaje_de_palabras_clave.str.replace(",", ".").astype(float)
  df.principales_palabras_clave = df.principales_palabras_clave.str.replace("\s+"," ").apply(lambda x : x[:-1])
  

  return df
