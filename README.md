# Proyecto de Análisis de Retención de Talento 🚀

Este proyecto tiene como objetivo identificar los factores clave que influyen en la fuga de empleados (attrition) en una empresa. A través de un análisis exhaustivo de datos de recursos humanos, se busca proporcionar insights accionables para mejorar la retención y el bienestar de los trabajadores.

## 📋 Estructura del Proyecto

El proyecto se divide en 5 fases principales que cubren desde la exploración inicial hasta la automatización del flujo de datos (ETL).

### Fase 1: EDA (Análisis Exploratorio de Datos) 🔍
En esta fase se realiza una primera aproximación a los datos originales (`hr.csv`):
- Análisis de la estructura del dataset (filas, columnas y tipos de datos).
- Identificación de valores nulos y duplicados.
- Análisis descriptivo de variables numéricas y categóricas para entender las distribuciones y detectar posibles anomalías.

### Fase 2: Transformación de Datos 🛠️
Limpieza y preparación del dataset para el análisis:
- **Estandarización**: Formateo de nombres de columnas y limpieza de strings en variables categóricas.
- **Cambios de tipo de dato**: Conversión de columnas a tipos adecuados (booleanos, enteros, strings).
- **Gestión de Nulos**: Imputación de valores faltantes utilizando diversas estrategias:
    - Mediana para ingresos mensuales.
    - Media (IterativeImputer/SimpleImputer) para edad y satisfacción.
    - Moda para variables categóricas como estado civil o departamento.
- **Eliminación de Redundancias**: Borrado de columnas con información irrelevante (ej. `Over18`, `StandardHours`).

### Fase 3: Visualización de Datos 📊
Exploración visual para encontrar patrones de fuga:
- **Insights clave**: Se identificó que la formación actúa como un "escudo" contra la rotación; los empleados sin formación en el último año tienen una tasa de fuga cercana al 30%.
- Hay relación entre satisfacción laboral, salario, horas extra y la decisión de abandonar la empresa.
- Comparativa entre perfiles que permanecen vs. perfiles que se van.

### Fase 4: Diseño y Creación de Base de Datos 🗄️
Modelado de datos para un almacenamiento eficiente y relacional:
- Diseño de un **Diagrama Entidad-Relación (ER)**.
- Creación de un esquema en MySQL (`hr_ABC`) con 5 tablas relacionadas:
    - `employees`: Datos personales y claves foráneas.
    - `job_details`: Información sobre el rol, departamento y viajes.
    - `career_history`: Historial de años en la empresa, promociones y attrition.
    - `compensation`: Detalles salariales.
    - `performance_satisfaction`: Métricas de desempeño y encuestas de satisfacción.

### Fase 5: ETL (Extract, Transform, Load) 🔄
Automatización de todo el proceso mediante scripts de Python para asegurar la escalabilidad:
- `main_limpieza.py`: Ejecuta todo el flujo de limpieza y guarda el archivo `df_hr_limpio.csv`.
- `main_bbdd.py`: Crea la estructura de tablas e inserta los datos limpios en la base de datos MySQL de forma automática.
- `src/`: Contiene los módulos de funciones auxiliares (`library_functions.py` y `bbdd_functions.py`).

---

## 🚀 Cómo ejecutar el proyecto

1. **Requisitos**: Instalar las librerías necesarias mediante:
   ```bash
   pip install -r ETL/requirements.txt
   ```
2. **Limpieza de datos**:
   ```bash
   python ETL/main_limpieza.py
   ```
3. **Carga en Base de Datos**: Asegúrate de tener MySQL corriendo y ejecuta:
   ```bash
   python ETL/main_bbdd.py
   ```

## 🛠️ Tecnologías utilizadas
- **Lenguaje**: Python 3.x
- **Librerías**: Pandas, NumPy, Seaborn, Matplotlib, Scikit-learn, MySQL-Connector.
- **Base de Datos**: MySQL Workbench.
- **Herramientas**: Jupyter Notebooks.
