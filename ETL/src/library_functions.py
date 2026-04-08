import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")
from sklearn.impute import SimpleImputer 
from sklearn.experimental import enable_iterative_imputer 
from sklearn.impute import IterativeImputer 
from sklearn.impute import KNNImputer 
pd.set_option("display.max_columns", None)

def exploracion_inicial(df,name="dataframe"):
    print(f"El dataframe '{name}' tiene {df.shape[0]} filas y {df.shape[1]} columnas.")
    print(df.head(2))
    print("="*150)
    print(df.tail(2))
    print("="*150)
    print(df.sample(2))
    print("="*150)
    print(f"Listamos abajo el conteo de no-nulos junto con el tipo de dato:")
    print("="*150)
    print(df.info())
    print("="*150)
    return df

def descriptive_analysis(df):
    print("NUMÉRICAS")
    print(df.describe(include="number").T)
    print("CATEGÓRICAS")
    print(df.describe(include="object").T)
    return df

def estandarizar_nombre(nombre):
    resultado = ""
    for i, letra in enumerate(nombre):
        # Si la letra es mayúscula y no es la primera, ponemos un "_"
        if letra.isupper() and i > 0:
            resultado += "_" + letra.lower()
        else:
            resultado += letra.lower()
    return resultado

def limpiar_categoricas(df):
    
    cols = df.select_dtypes(include="object").columns
    
    for col in cols:
        df[col] = df[col].str.strip().str.title().str.replace("-","_")
        if col == "marital_status":
            df[col] = df[col].str.replace('Marreid','Married')

    return df

def eliminar_duplicados(df):

    df = df.drop_duplicates()
    print(df.shape)

    return df

def convertir_a_int(df, columns):
    df[columns] = df[columns].astype("Int64")
    return df

def convertir_a_bool(df, columnas):

    for columna in columnas:
        df[columna] = df[columna].str.strip().str.lower().map({"yes": True, "no": False})
        df[columna] = df[columna].astype("boolean")
    return df

def convertir_a_str(df, columns):
    df[columns] = df[columns].astype(object)
    return df

def porcentaje_nulos_num(df):

    nulos_num = (df.select_dtypes(include="number").isnull().sum() / df.shape[0] * 100).sort_values(ascending=False)

    nulos_num = nulos_num.reset_index()

    nulos_num.columns = ["nombre_columna", "%_nulos"]

    return nulos_num

def media_mediana(df, columna):

    media = df[columna].mean()
    mediana = df[columna].median()

    return media, mediana


def imputar_mediana(df, columna):

    objeto_imputacion_simple = SimpleImputer(strategy="median")

    valores_imputados = objeto_imputacion_simple.fit_transform(df[[columna]])

    df[columna] = valores_imputados

    return df

def imputar_media_sklearn(df, columnas):
    # df = df.copy()

    # crear objeto imputador
    imputador = SimpleImputer(strategy="mean")

    # aplicar imputación a varias columnas
    datos_imputados = imputador.fit_transform(df[columnas])

    # volver a asignar al dataframe
    df[columnas] = datos_imputados
    
    for columna in columnas:
        df[columna] = df[columna].round().astype("Int64")

    # comprobar nulos
    print("Nulos después de imputar:")
    print(df[columnas].isnull().sum())

    return df


def porcentaje_nulos_cat(df):

    nulos_cat = (df.select_dtypes(include="object").isnull().sum() / df.shape[0] * 100).sort_values(ascending=False)

    nulos_cat = nulos_cat.reset_index()

    nulos_cat.columns = ["nombre_columna", "%_nulos"]

    mascara_nulos = nulos_cat["%_nulos"] > 0

    nulos_cat = nulos_cat[mascara_nulos]

    return nulos_cat

def imputacion_cat_moda(df, columns):
    for col in columns:
        moda = df[col].mode()[0]
        df[col] = df[col].fillna(moda)
        print(f"El numero total de nulos en la columna {col} es {df[col].isnull().sum()}")
    return df  


def imputacion_cat_desconocido(df, columns):
    for col in columns:
        df[col] = df[col].fillna("Unknown")
        print(f"El numero total de nulos en la columna {col} es {df[col].isnull().sum()}")
    return df

def imputacion_overtime_moda(df, column):
    moda = df[column].mode()[0]
    df[column] = df[column].fillna(moda)
    print(f"EL numero total de nulos en la columna {column} es {df[column].isnull().sum()}")
    return df

def borrar_columnas(df, columns):
    df.drop(columns, inplace=True, errors="ignore", axis=1)
    return df