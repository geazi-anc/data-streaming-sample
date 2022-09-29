from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (SparkSession.builder
         .appName("Words Count Analysis")
         .getOrCreate()
         )

df1 = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:29092")
       .option("subscribe", "words")
       .option("startingOffsets", "earliest")
       .load()
       )

df2 = df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

lines = df2.select(F.explode(F.split(df2.value, " ")).alias("word"))

word_counts = (lines.groupBy("word")
               .count()
               .orderBy("count", ascending=False)
               )

(word_counts.writeStream
 .format("console")
 .outputMode("complete")
 .start()
 .awaitTermination()
 )
