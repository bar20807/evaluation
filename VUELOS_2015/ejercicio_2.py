from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc

spark = SparkSession.builder \
    .appName("AnalisisVuelos2015") \
    .getOrCreate()

file_path = "/FileStore/tables/data_ejercicio_2/On_Time_Reporting_Carrier_On_Time_Performance__1987_present__2015_1.csv"

flight_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# 3. Calcular retraso promedio en llegada para un aeropuerto específico
aeropuerto_objetivo = "LAX"  # Acá se ingresa el aeropuerto específico que se desea consultar

resultado_retrasos = flight_df \
    .filter(f"Dest = '{aeropuerto_objetivo}'") \
    .select(avg("ArrDelay").alias("RetrasoPromedio")) \
    .first()

print(f"\nRetraso promedio en llegada a {aeropuerto_objetivo}: {resultado_retrasos[0]:.2f} minutos\n")

# 4. Encontrar las 10 rutas más populares
print("Top 10 rutas más frecuentes:")
top_rutas = flight_df \
    .groupBy("Origin", "Dest") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)

top_rutas.show(truncate=False)