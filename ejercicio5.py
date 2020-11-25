# Databricks notebook source
# MAGIC %md
# MAGIC # Ejercicio 4
# MAGIC ### Universidad del Valle de Guatemala
# MAGIC ### Big Data
# MAGIC ### Alexis Fernando Hengstenberg Chocooj

# COMMAND ----------

# MAGIC %md 
# MAGIC buscamos las tablas

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/dataej4

# COMMAND ----------

# MAGIC %md
# MAGIC cargamos los archivos

# COMMAND ----------

results = spark.read.csv('/FileStore/tables/dataej4/results.csv', header=True, inferSchema=True, nullValue=r'\N')
drivers = spark.read.csv('/FileStore/tables/dataej4/drivers.csv', header=True, inferSchema=True, nullValue=r'\N')
races = spark.read.csv('/FileStore/tables/dataej4/races.csv', header=True, inferSchema=True, nullValue=r'\N')
qualifying = spark.read.csv('/FileStore/tables/dataej4/qualifying.csv', header=True, inferSchema=True, nullValue=r'\N')
lap_times = spark.read.csv('/FileStore/tables/dataej4/lap_times.csv', header=True, inferSchema=True, nullValue=r'\N')
circuits = spark.read.csv('/FileStore/tables/dataej4/circuits.csv', header=True, inferSchema=True, nullValue=r'\N')
pit_stops = spark.read.csv('/FileStore/tables/dataej4/pit_stops.csv', header=True, inferSchema=True, nullValue=r'\N')
constructors = spark.read.csv('/FileStore/tables/dataej4/constructors.csv', header=True, inferSchema=True, nullValue=r'\N')

# COMMAND ----------

# MAGIC %md
# MAGIC ejercicio1

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.functions import col, lit

datan = results.withColumn('diferencias', col('grid') - col('position'))
display(datan.join(races, 'raceId').where(col('year') > 2011)
              .groupBy('circuitId')
              .agg(
                f.sum('diferencias').alias('sum_dif_pos'),
                f.sum(f.when(col('diferencias') >= 0, col('diferencias'))).alias('sum_dif_positivos'),
                f.avg(f.when(col('diferencias') >= 0, col('diferencias'))).alias('avg_dif_positivo'))
              .join(circuits, 'circuitId')
              .orderBy(col('avg_dif_positivo'))
             )


# COMMAND ----------

# MAGIC %md 
# MAGIC ejercicio 2

# COMMAND ----------

display(qualifying.withColumn('q1 milesecond', col('q1').substr(0, 1) * 60000 + col('q1').substr(3, 2) * 1000 + col('q1').substr(6, 3))
           .withColumn('q2 milesecond', col('q2').substr(0, 1) * 60000 + col('q2').substr(3, 2) * 1000 + col('q2').substr(6, 3))
           .withColumn('q3 milesecond', col('q3').substr(0, 1) * 60000 + col('q3').substr(3, 2) * 1000 + col('q3').substr(6, 3))
)

# COMMAND ----------

display(lap_times.withColumn(
  'ms', col('milliseconds')
))

# COMMAND ----------

# MAGIC %md
# MAGIC ejercicio 3

# COMMAND ----------

display(drivers)

# COMMAND ----------

display(lap_times.withColumnRenamed(
  'position', 'lap_position',
))

# COMMAND ----------

# MAGIC %md
# MAGIC ejercicio 4

# COMMAND ----------

display(constructors)

# COMMAND ----------

# MAGIC %md
# MAGIC ejercicio 5

# COMMAND ----------

display(lap_times)

# COMMAND ----------

question05 = (
  datan
    .join(races, 'raceId')
    .join(lap_times, ['driverId, raceId'])
    .where(col('year') == 2010)
    .where(col('position') > 0)
    .where(col('position') < 11)
    .select(
      'lap',
      'milliseconds',
      'position'
    )
    .orderBy('lap')
)

