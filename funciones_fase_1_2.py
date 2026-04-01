def exploracion_inicial(df,name="dataframe"):
    print(f"El dataframe '{name}' tiene {df.shape[0]} filas y {df.shape[1]} columnas.")
    display(df.head(2))
    print("="*150)
    display(df.tail(2))
    print("="*150)
    display(df.sample(2))
    print("="*150)
    print(f"Listamos abajo el conteo de no-nulos junto con el tipo de dato:")
    print("="*150)
    print(df.info())
    print("="*150)


def descriptive_analysis(df):
    print("NUMÉRICAS")
    display(df.describe(include="number").T)
    print("CATEGÓRICAS")
    display(df.describe(include="object").T)

def estandarizar_nombre(nombre):
    resultado = ""
    for i, letra in enumerate(nombre):
        # Si la letra es mayúscula y no es la primera, ponemos un "_"
        if letra.isupper() and i > 0:
            resultado += "_" + letra.lower()
        else:
            resultado += letra.lower()
    return resultado