# Importar librerías necesarias
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("IntegracionPandasSpark") \
    .getOrCreate()

# ---------------------------------------
# 1. Crear DataFrame de películas con Pandas
# ---------------------------------------
datos_peliculas = pd.DataFrame({
    'ID': [1, 2, 3, 4],
    'Título': ['Película1', 'Película2', 'Película3', 'Película4'],
    'Año': [2020, 2019, 2021, 2018]
})

# ---------------------------------------
# 2. Crear DataFrame de críticas con Spark
# ---------------------------------------
criticas_data = [
    (1, 'Critico1', 4.5),
    (2, 'Critico2', 3.8),
    (3, 'Critico1', 4.2),
    (4, 'Critico3', 4.7)
]
criticas = spark.createDataFrame(criticas_data, ['PeliculaID', 'Critico', 'Puntuacion'])

# ---------------------------------------
# 3. Calcular el promedio de puntuación por película
# ---------------------------------------
promedios = criticas.groupBy("PeliculaID") \
    .agg(avg("Puntuacion").alias("Puntuacion_Promedio"))

# ---------------------------------------
# 4. Convertir el DataFrame de Pandas a Spark
# ---------------------------------------
datos_peliculas_spark = spark.createDataFrame(datos_peliculas)

# ---------------------------------------
# 5. Hacer el join entre películas y promedios de críticas
# ---------------------------------------
resultado = datos_peliculas_spark.join(
    promedios,
    datos_peliculas_spark.ID == promedios.PeliculaID
).select(
    "Título", "Año", "Puntuacion_Promedio"
)


resultado.show()
