// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("LogsSpark").getOrCreate()

// COMMAND ----------

import org.apache.spark.sql.functions._

val logsPath = "/FileStore/tables/access_log_Aug95"
val logsDF = spark.read.text(logsPath)
logsDF.show(5,false)

// COMMAND ----------

//regex                           domain - - [datetime -timezone(omitimos)] "request_method" http_status_code size
//tiene un formato similar asi in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839

val domain = "\\S+" //Lo que no sea un espacio
val userId = "(\\s\\-){2}" //equivale a esto pero de una forma más optima "\\s\\-\\s\\-"
val dateTime = "(\\d{2})/(\\w{3})/(\\d{4})((:(\\d{2})){3})" //omitimos el timezone
val requestMethod = "\"(\\S+)\\s"
val resource = "\\s(/\\S+)"
val protocol = "\\HTTP/\\d\\.\\d" 
val status_code = "\\s(\\d{3})\\s" //un codigo de estado de HTTP, siempre esta compuesto por 3 digitos
val size = "(\\d+$)|(\\-$)" //$ -> termina en un digito, tener en cuenta que puede aber un - cuando no hay un tamaño


val logsDFmod = logsDF
  .select(
    regexp_extract($"value", domain, 0).alias("domain"), //value -> nombre generico, al no tener un header
    regexp_extract($"value", userId, 0).alias("userIdTotal"),
    regexp_extract($"value", dateTime, 0).alias("datime"),
    regexp_extract($"value", requestMethod, 1).alias("requestMethod"),
    regexp_extract($"value", resource, 1).alias("resource"),
    regexp_extract($"value", protocol, 0).alias("protocol"),
    regexp_extract($"value", status_code, 1).alias("status_code"),
    regexp_extract($"value", size, 0).alias("size"),
  )

logsDFmod.show(false)

// COMMAND ----------

//save the file format parquet 
val saveDF = logsDFmod
  .write
  .option("header", "true")
  .option("delimiter", ";")
  .mode("overwrite")
  .parquet("/FileStore/tables/logsParquet")

// COMMAND ----------

val logsPRQ = spark.read
  .format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ";")
  .load("/FileStore/tables/logsParquet")
display(logsPRQ)

// COMMAND ----------

//logsPRQ.describe()
logsPRQ.show()

// COMMAND ----------

//¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.
logsPRQ.createOrReplaceTempView("logsPRQ_t")
val query1 = spark.sql("SELECT DISTINCT protocol FROM logsPRQ_t")

query1.show()

// COMMAND ----------

//¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos
val query2 = logsPRQ
  .groupBy("status_code")  
  .agg(
    count($"status_code").alias("cont_status_code")
  )
  .orderBy($"cont_status_code".desc)

query2.show()

// COMMAND ----------

//¿Y los métodos de petición (verbos) más utilizados?
val query3 = logsPRQ
  .groupBy("requestMethod")
  .agg(
    count($"requestMethod").alias("cont_requestMethod")
  )
  .orderBy($"cont_requestMethod".desc)

query3.show()

// COMMAND ----------

//¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
val query4 = logsPRQ
  .groupBy("domain")
  .agg(
    sum($"size").alias("sum_size"),
  )
  .orderBy($"sum_size".desc)
  
query4.show(1)

// COMMAND ----------

//Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log.

val query5 = logsPRQ
  .groupBy("domain")
  .agg(
    count($"resource").alias("cont_resource")
  )
  .orderBy($"cont_resource".desc)

query5.show(1)

// COMMAND ----------

//¿Qué días la web recibió más tráfico?
val query6 = logsPRQ
  .withColumn("datetype", to_date(col("datime"),"dd/MMM/yyyy:HH:mm:ss"))
  .groupBy("datetype")
  .agg(
    count($"domain").alias("count_domain")
  )
  .orderBy($"count_domain".desc)

query6.show()

// COMMAND ----------

//¿Cuáles son los hosts son los más frecuentes?
val query7 = logsPRQ
  .groupBy("domain")
  .agg(
    count($"domain").alias("count_domain")
  )
  .orderBy($"count_domain".desc)

query7.show()

// COMMAND ----------

//¿A qué horas se produce el mayor número de tráfico en la web?
val query8 = logsPRQ
  .withColumn("tiempo", to_timestamp(col("datime"), "dd/MMM/yyy:HH:mm:ss"))
  .withColumn("horas", hour(col("tiempo")))
  .drop("tiempo")
  .groupBy("horas")
  .agg(
    count($"horas").alias("count_horas")
  )
  .orderBy("count_horas")

query8.show()

// COMMAND ----------

//¿Cuál es el número de errores 404 que ha habido cada día?
val query9 = logsPRQ
  .withColumn("fecha", to_date(col("datime"), "dd/MMM/yyy:HH:mm:ss"))
  .groupBy("fecha")
  .agg(
    count(
      when($"status_code" === "404", $"status_code")).alias("count_status_code")
  )
  .orderBy($"count_status_code".desc)
  

query9.show()

// COMMAND ----------


