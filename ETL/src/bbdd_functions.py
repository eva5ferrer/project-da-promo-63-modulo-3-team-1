import mysql.connector
from mysql.connector import errorcode
import pandas as pd


def conectar_mysql(password):
    try:
        cnx = mysql.connector.connect(
            user='root',
            password=password,
            host='127.0.0.1'
            # auth_plugin = 'mysql_native_password'
        )
        print("¡Conexión exitosa a MySQL!")
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Algo está mal con tu nombre de usuario o contraseña.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("La base de datos no existe.")
        else:
            print(err) #
            print("Código de Error:", err.errno) #
            print("SQLSTATE", err.sqlstate) #
            print("Mensaje", err.msg) #
        return None


def crear_schema_y_cursor(cnx, schema="hr_ABC_ETL"):
    mycursor = cnx.cursor()
    mycursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    mycursor.execute(f"USE {schema}")
    return mycursor


def eliminar_tablas(mycursor, cnx):
    mycursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for t in ["employees", "job_details", "career_history", "compensation", "performance_satisfaction"]:
        mycursor.execute(f"DROP TABLE IF EXISTS {t}")
    mycursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    cnx.commit()
    print("Tablas eliminadas.")


def crear_tablas(mycursor, cnx):
    mycursor.execute("""CREATE TABLE IF NOT EXISTS job_details (
        job_id INT NOT NULL AUTO_INCREMENT,
        department VARCHAR(50) NOT NULL,
        job_role VARCHAR(50) NOT NULL,
        job_level INT NOT NULL,
        business_travel VARCHAR(50) NOT NULL,
        over_time BOOLEAN NOT NULL,
        PRIMARY KEY (job_id)
    ) ENGINE=InnoDB;""")

    mycursor.execute("""CREATE TABLE IF NOT EXISTS career_history (
        career_id INT NOT NULL AUTO_INCREMENT,
        total_working_years INT NOT NULL,
        years_at_company INT NOT NULL,
        years_in_current_role INT NOT NULL,
        years_since_last_promotion INT NOT NULL,
        years_with_curr_manager INT NOT NULL,
        num_companies_worked INT NOT NULL,
        training_times_last_year INT NOT NULL,
        attrition BOOLEAN NOT NULL,
        PRIMARY KEY (career_id)
    ) ENGINE=InnoDB;""")

    mycursor.execute("""CREATE TABLE IF NOT EXISTS compensation (
        compensation_id INT NOT NULL AUTO_INCREMENT,
        monthly_income INT NOT NULL,
        monthly_rate INT NOT NULL,
        percent_salary_hike INT NOT NULL,
        PRIMARY KEY (compensation_id)
    ) ENGINE=InnoDB;""")

    mycursor.execute("""CREATE TABLE IF NOT EXISTS performance_satisfaction (
        performance_satisfaction_id INT NOT NULL AUTO_INCREMENT,
        performance_rating INT NOT NULL,
        job_involvement INT NOT NULL,
        environment_satisfaction INT NOT NULL,
        relationship_satisfaction INT NOT NULL,
        job_satisfaction INT NOT NULL,
        work_life_balance INT NOT NULL,
        PRIMARY KEY (performance_satisfaction_id)
    ) ENGINE=InnoDB;""")

    mycursor.execute("""CREATE TABLE IF NOT EXISTS employees (
        employee_number VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        gender VARCHAR(20) NOT NULL,
        marital_status VARCHAR(40) NOT NULL,
        education INT NOT NULL,
        education_field VARCHAR(70) NOT NULL,
        distance_from_home INT NOT NULL,
        job_id INT NOT NULL,
        career_id INT NOT NULL,
        compensation_id INT NOT NULL,
        performance_satisfaction_id INT NOT NULL,
        PRIMARY KEY (employee_number),
        FOREIGN KEY (job_id) REFERENCES job_details(job_id),
        FOREIGN KEY (career_id) REFERENCES career_history(career_id),
        FOREIGN KEY (compensation_id) REFERENCES compensation(compensation_id),
        FOREIGN KEY (performance_satisfaction_id) REFERENCES performance_satisfaction(performance_satisfaction_id)
    ) ENGINE=InnoDB;""")

    cnx.commit()
    print("Tablas creadas.")


def preparar_columnas(df_hr):
    df_hr = df_hr.copy()
    df_hr["attrition"] = df_hr["attrition"].map({"Yes": True, "No": False, True: True, False: False})
    return df_hr


def insertar_datos(cursor, conexion, query, dataframe, nombre_tabla):
    try:
        valores = []
        for fila in dataframe.itertuples(index=False, name=None):
            fila_limpia = tuple(x.item() if hasattr(x, "item") else x for x in fila)
            valores.append(fila_limpia)
        cursor.executemany(query, valores)
        conexion.commit()
        print(f"Datos insertados en {nombre_tabla}")
    except mysql.connector.Error as err:
        print(f"Error al insertar en {nombre_tabla}: {err}")


def insertar_todos_los_datos(mycursor, cnx, df_hr):

    # Preparamos los DataFrames
    df_job_details = df_hr[["department", "job_role", "job_level", "business_travel", "over_time"]].drop_duplicates().reset_index(drop=True)
    df_job_details["over_time"] = df_job_details["over_time"].map({"Yes": 1, "No": 0, True: 1, False: 0}).astype(int)

    df_career_history = df_hr[["total_working_years", "years_at_company", "years_in_current_role",
                                "years_since_last_promotion", "years_with_curr_manager",
                                "num_companies_worked", "training_times_last_year", "attrition"]].drop_duplicates().reset_index(drop=True)
    df_career_history["attrition"] = df_career_history["attrition"].map({"Yes": 1, "No": 0, True: 1, False: 0}).fillna(0).astype(int)

    df_compensation = df_hr[["monthly_income", "monthly_rate", "percent_salary_hike"]].drop_duplicates().reset_index(drop=True)

    df_performance_satisfaction = df_hr[["performance_rating", "job_involvement", "environment_satisfaction",
                                          "relationship_satisfaction", "job_satisfaction", "work_life_balance"]].drop_duplicates().reset_index(drop=True)

    # Insertamos tablas auxiliares
    insertar_datos(mycursor, cnx,
        "INSERT INTO job_details (department, job_role, job_level, business_travel, over_time) VALUES (%s, %s, %s, %s, %s)",
        df_job_details, "job_details")

    insertar_datos(mycursor, cnx,
        "INSERT INTO career_history (total_working_years, years_at_company, years_in_current_role, years_since_last_promotion, years_with_curr_manager, num_companies_worked, training_times_last_year, attrition) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        df_career_history, "career_history")

    insertar_datos(mycursor, cnx,
        "INSERT INTO compensation (monthly_income, monthly_rate, percent_salary_hike) VALUES (%s, %s, %s)",
        df_compensation, "compensation")

    insertar_datos(mycursor, cnx,
        "INSERT INTO performance_satisfaction (performance_rating, job_involvement, environment_satisfaction, relationship_satisfaction, job_satisfaction, work_life_balance) VALUES (%s, %s, %s, %s, %s, %s)",
        df_performance_satisfaction, "performance_satisfaction")

    # Recuperamos IDs desde MySQL
    mycursor.execute("SELECT * FROM job_details")
    df_job_ids = pd.DataFrame(mycursor.fetchall(), columns=["job_id", "department", "job_role", "job_level", "business_travel", "over_time"])

    mycursor.execute("SELECT * FROM career_history")
    df_career_ids = pd.DataFrame(mycursor.fetchall(), columns=["career_id", "total_working_years", "years_at_company",
        "years_in_current_role", "years_since_last_promotion", "years_with_curr_manager",
        "num_companies_worked", "training_times_last_year", "attrition"])

    mycursor.execute("SELECT * FROM compensation")
    df_compensation_ids = pd.DataFrame(mycursor.fetchall(), columns=["compensation_id", "monthly_income", "monthly_rate", "percent_salary_hike"])

    mycursor.execute("SELECT * FROM performance_satisfaction")
    df_performance_ids = pd.DataFrame(mycursor.fetchall(), columns=["performance_satisfaction_id", "performance_rating",
        "job_involvement", "environment_satisfaction", "relationship_satisfaction", "job_satisfaction", "work_life_balance"])

    # Convertimos tipos para que el merge funcione
    df_job_ids["over_time"] = df_job_ids["over_time"].astype(bool)
    df_career_ids["attrition"] = df_career_ids["attrition"].astype(bool)

    # Mergeamos IDs en df_hr
    df_hr = df_hr.merge(df_job_ids, on=["department", "job_role", "job_level", "business_travel", "over_time"], how="left")
    df_hr = df_hr.merge(df_career_ids, on=["total_working_years", "years_at_company", "years_in_current_role",
        "years_since_last_promotion", "years_with_curr_manager", "num_companies_worked",
        "training_times_last_year", "attrition"], how="left")
    df_hr = df_hr.merge(df_compensation_ids, on=["monthly_income", "monthly_rate", "percent_salary_hike"], how="left")
    df_hr = df_hr.merge(df_performance_ids, on=["performance_rating", "job_involvement", "environment_satisfaction",
        "relationship_satisfaction", "job_satisfaction", "work_life_balance"], how="left")

    # Insertamos employees
    df_employees = df_hr[["employee_number", "age", "gender", "marital_status", "education",
                            "education_field", "distance_from_home", "job_id", "career_id",
                            "compensation_id", "performance_satisfaction_id"]]

    insertar_datos(mycursor, cnx,
        "INSERT INTO employees (employee_number, age, gender, marital_status, education, education_field, distance_from_home, job_id, career_id, compensation_id, performance_satisfaction_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        df_employees, "employees")


def cerrar_conexion(mycursor, cnx):
    mycursor.close()
    cnx.close()
    print("Conexión cerrada.")