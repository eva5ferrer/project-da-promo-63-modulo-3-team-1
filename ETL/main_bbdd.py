from src import bbdd_functions as bbdd
import pandas as pd

# Cargamos y preparamos datos
df_hr = pd.read_csv("files/df_hr_limpio.csv")
df_hr = bbdd.preparar_columnas(df_hr)

# Conexión a base de datos
password = input("Escribe aquí tu contraseña: ")
cnx = bbdd.conectar_mysql(password)
mycursor = bbdd.crear_schema_y_cursor(cnx, "hr_ABC_ETL")

# Eliminamos y creamos las tablas
bbdd.eliminar_tablas(mycursor, cnx)
bbdd.crear_tablas(mycursor, cnx)

# Insertamos todos los datos
bbdd.insertar_todos_los_datos(mycursor, cnx, df_hr)

# Cerramos conexión
bbdd.cerrar_conexion(mycursor, cnx)

print("ETL COMPLETADA, BBDD CREADA")