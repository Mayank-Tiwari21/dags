from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum, round,max,asc

spark = SparkSession.builder.appName("SparkJobOnEMR").getOrCreate()
'''
# df1 =  spark.read.csv("s3a://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset1/",inferSchema = True, header = True)
# df2 = spark.read.csv("s3a://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset2/",inferSchema = True,header = True)

df1 = spark.read.parquet("s3a://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset1/*")
df2 = spark.read.parquet("s3a://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset2/*")



# applying for the confirmedForeignNationalPer
total_ConfirmedForeignNational = df1.agg(sum("ConfirmedForeignNational")).collect()[0][0]
df1_2 = df1.withColumn("ConfirmedForeignNationalPer",round(col("ConfirmedForeignNational")/total_ConfirmedForeignNational*100,2))


#state_unionterritory with he highest positive case percentage at any given date¶
df2_2 = df2.withColumn("PositiveRate",col("Positive")/col("TotalSamples")*100)
date_wise_max = df2_2.groupBy("Date").agg(
    max("PositiveRate").alias("PositiveRate")
).orderBy("Date")

df2_3 = df2_2.join(date_wise_max,["Date","PositiveRate"],"inner").select(df2_2["Date"],df2_2["State"]).orderBy("Date").show()




#writing the final data to the bucket-silver in parquet
df1_2.write.format('parquet').mode("overwrite").save("s3://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset1/")
df2_3.write.format('parquet').mode('overwrite').save("s3://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset2/")

'''




df1 = spark.read.parquet("s3a://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset1/")
df2 = spark.read.parquet("s3a://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset2/")



# applying for the confirmedForeignNationalPer
total_ConfirmedForeignNational = df1.agg(sum("ConfirmedForeignNational")).collect()[0][0]
df1_2 = df1.withColumn("ConfirmedForeignNationalPer",round(col("ConfirmedForeignNational")/total_ConfirmedForeignNational*100,2))


#state_unionterritory with he highest positive case percentage at any given date¶
df2_2 = df2.withColumn("PositiveRate",col("Positive")/col("TotalSamples")*100)
date_wise_max = df2_2.groupBy("Date").agg(
    max("PositiveRate").alias("PositiveRate")
).orderBy("Date")

df2_3 = df2_2.join(date_wise_max,["Date","PositiveRate"],"inner").select(df2_2["Date"],df2_2["State"]).orderBy("Date")




#writing the final data to the bucket-silver in parquet
df1_2.write.format('parquet').mode("append").save("s3a://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset1/")
df2_3.write.format('parquet').mode('append').save("s3a://ttn-de-bootcamp-2025-silver-us-east-1/mayank/assessment4/data/silver/glue/dataset2/")
