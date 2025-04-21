from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import broadcast
from pyspark import StorageLevel
from pyspark.sql.types import *

file_path1 = ""
file_path2 = ""


answer3=""
answer4=""


spark = SparkSession.builder.appName("assessment").getOrCreate()
print(spark)
#2.1
spark = SparkSession.builder.appName("assessment").getOrCreate()
x_df = spark.read.csv(file_path1,header =True,inferSchema = True)

y_df = spark.read.csv(file_path2,header =True,inferSchema =True)


#2.2
x_df1 = x_df.withColumn("amount",when(isnull(col("amount")),10).otherwise(col("amount")))
y_df1 = y_df.withColumn("amount",when(isnull(col("amount")),10).otherwise(col("amount")))

x_df2 = x_df1.filter(isnotnull(col("invoice_no")))\
    .filter(isnotnull(col("gstin")))\
    .filter(isnotnull(col("supplier_gstin")))\
    .filter(isnotnull(col("invoice_date")))

y_df2 = y_df1.filter(isnotnull(col("invoice_no")))\
    .filter(isnotnull(col("gstin")))\
    .filter(isnotnull(col("supplier_gstin")))\
    .filter(isnotnull(col("invoice_date")))

x_df3 = x_df2.groupBy([col("invoice_no"),col("gstin"),col("supplier_gstin"),col("invoice_date")])\
    .agg(
        sum("amount").alias("amount"),
        sum("gst").alias("gst")
    )
y_df3 = y_df2.groupBy([col("invoice_no"),col("gstin"),col("supplier_gstin"),col("invoice_date")])\
    .agg(
        sum("amount").alias("amount"),
        sum("gst").alias("gst")
    )


#2.3
x_df4= x_df3.withColumn("total_amount",col("amount")+col("gst"))
y_df4= y_df3.withColumn("total_amount",col("amount")+col("gst"))

#2.4
x_df5 = x_df4.groupby([col("gstin"),col("supplier_gstin"),col("invoice_date")])\
.agg(
    sum("amount").alias("amount"),
    sum("gst").alias("gst"),
    sum("total_amount").alias("total_amount")
)

y_df5 = y_df4.groupby([col("gstin"),col("supplier_gstin"),col("invoice_date"),col("invoice_no")])\
.agg(
    sum("amount").alias("amount"),
    sum("gst").alias("gst"),
    sum("total_amount").alias("total_amount")
)


#2.5
@udf
def run_length(string_entered):
    marker = string_entered[0]
    counter = 0
    return_str = ""
    for char in string_entered:
        if (marker == char):
            counter +=1
        else:
            str = f"{marker}{counter}"
            return_str =return_str + str
            marker = char
            counter = 1
    str = f"{marker}{counter}"
    return_str =return_str + str
    return return_str

x_df6= x_df5.withColumn("supplier_gstin",run_length(col("supplier_gstin")))
y_df6= y_df5.withColumn("supplier_gstin",run_length(col("supplier_gstin")))


x_df7 = x_df6.withColumn("supplier_gstin",md5(col("supplier_gstin")))
y_df7 = y_df6.withColumn("supplier_gstin",md5(col("supplier_gstin")))


#2.6

type_A = x_df4.join(y_df4,["invoice_no","gstin","supplier_gstin","invoice_date","amount"],"inner").select("invoice_no","gstin","supplier_gstin","invoice_date")
type_A = type_A.withColumn("category",lit("Type A"))

type_e = x_df4.join(y_df4,["invoice_no","gstin","supplier_gstin","amount"],"leftanti").select("invoice_no","gstin","supplier_gstin","invoice_date")
type_e = type_e.withColumn("category",lit("Type E"))

type_d = y_df4.join(x_df4,["invoice_no","gstin","supplier_gstin","amount"],"leftanti").select("invoice_no","gstin","supplier_gstin","invoice_date")
type_d = type_d.withColumn("category",lit("Type D"))

type_b = x_df4.join(y_df4,
                    (x_df4["invoice_no"]==y_df4["invoice_no"])&(x_df4["gstin"]==y_df4["gstin"])&(x_df4["supplier_gstin"]==y_df4["supplier_gstin"])&(x_df4["invoice_date"]==y_df4["invoice_date"])&( (x_df4["amount"]-y_df4["amount"]<=50) | (x_df4["amount"]-y_df4["amount"]>=-50)),
                    "inner").select(x_df4["invoice_no"],x_df4["gstin"],x_df4["supplier_gstin"],x_df4["invoice_date"])
type_b = type_b.withColumn("category",lit("Type B"))


type_c = x_df4.join(y_df4,
                    (x_df4["invoice_no"]==y_df4["invoice_no"])&(x_df4["gstin"]==y_df4["gstin"])&(x_df4["supplier_gstin"]==y_df4["supplier_gstin"])&(x_df4["invoice_date"]==y_df4["invoice_date"])&( (x_df4["amount"]-y_df4["amount"]>50) | (x_df4["amount"]-y_df4["amount"]<-50)),
                    "inner").select(x_df4["invoice_no"],x_df4["gstin"],x_df4["supplier_gstin"],x_df4["invoice_date"])
type_c = type_c.withColumn("category",lit("Type C"))

unioned_df = type_A.union(type_b).union(type_c).union(type_d).union(type_e)
# unioned_df.write.format("csv").option("header","True").mode("append").save("file:///home/hdoop/notebooks/answer3.csv")
unioned_df.write.format("csv").option("header","True").mode("append").save(answer3)


df = unioned_df.join(x_df4,["invoice_no","gstin","supplier_gstin"]).select(col("total_amount").alias("X_amt"),"category")
df2 = unioned_df.join(y_df4,["invoice_no","gstin","supplier_gstin"]).select(col("total_amount").alias("y_amt"),"category")
df3 = df.join(df2,on = ["category"])
df4 = df3.groupBy("category").agg(
    round(sum("X_amt"),2).alias("total_amount_X"),
    round(sum("y_amt"),2).alias("total_amouunt_Y")
).drop("X_amt").drop("y_amt")
# df4.write.format("csv").option("header","True").mode("append").save("file:///home/hdoop/notebooks/answer4.csv")
df4.write.format("csv").option("header","True").mode("append").save(answer4)
