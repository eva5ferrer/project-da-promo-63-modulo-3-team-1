from src import library_functions as lf
import pandas as pd

df_hr = pd.read_csv("files/hr.csv")

# Exploracion inicial
df_hr = lf.exploracion_inicial(df_hr)
df_hr = lf.descriptive_analysis(df_hr)

# Estandarizamos nombres de columnas
df_hr.columns = [lf.estandarizar_nombre(col) for col in df_hr.columns]
df_hr = lf.limpiar_categoricas(df_hr)
df_hr = lf.eliminar_duplicados(df_hr)
df_hr = lf.convertir_a_int(df_hr, ["age", "job_satisfaction", "training_times_last_year"])
df_hr = lf.convertir_a_bool(df_hr, ["attrition", "over_time"])
df_hr = lf.convertir_a_str(df_hr, ["employee_number"])

# Gestionamos nulos numéricos
print(lf.porcentaje_nulos_num(df_hr))
print(lf.media_mediana(df_hr, "monthly_income"))
df_hr = lf.imputar_mediana(df_hr, "monthly_income")
df_hr = lf.imputar_media_sklearn(df_hr, ["age", "job_satisfaction", "training_times_last_year", "standard_hours", "years_with_curr_manager"])

# Gestionamos nulos categóricos
print(lf.porcentaje_nulos_cat(df_hr))
df_hr = lf.imputacion_cat_moda(df_hr, ["business_travel", "marital_status", "department"])
df_hr = lf.imputacion_cat_desconocido(df_hr, ["education_field"])
df_hr = lf.imputacion_overtime_moda(df_hr, "over_time")

# Verificamos que no quedan nulos
print(df_hr.isnull().sum()[df_hr.isnull().sum() > 0])

# Guardamos resultado
df_hr_limpio = df_hr.copy()
df_hr_limpio.to_csv("files/df_hr_limpio.csv", index=False)