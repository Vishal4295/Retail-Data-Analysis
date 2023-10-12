# Import all Required Libraries

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, window, sum, count, udf
from pyspark.sql.types import StringType, ArrayType, DoubleType, TimestampType, StructType, StructField, IntegerType

# Create a Spark Session

spark = SparkSession  \
	.builder  \
	.appName("RetailProject")  \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled","false") \
	.getOrCreate()

# Read data 

messages = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","real-time-project") \
	.load()

# Define Schema for columns
schema = StructType()
items_schema = StructType()


# Define Schema for fields inside "items" array

items_schema.add("SKU", StringType(), False)
items_schema.add("title", StringType(), False)
items_schema.add("unit_price", DoubleType(), False)
items_schema.add("quantity", DoubleType(), False)

# define schema for entire message

schema.add("invoice_no", StringType(), False)
schema.add("timestamp", TimestampType(), False)
schema.add("country", StringType(), False)
schema.add("type", StringType(), False)
schema.add("items", ArrayType(items_schema), False)




# Explode nested fields as rows
messages_df = messages.select(from_json(col("value").cast("String"), schema).alias("data")).select(col("data.*"))
messages_df = messages_df.select(col("*"), explode("items").alias("item"))
messages_df = messages_df.select("invoice_no","timestamp","country","type", col("item.*"))
messages_df.printSchema()




# Register a temp table

messages_df.createOrReplaceTempView("orders_data")
# Functions to calclulate KPI

def total_cost(unit_price, quantity, type):
    if type=='ORDER':
        return round(unit_price*quantity,2)
    else:
        return round(-1*unit_price*quantity,2)

def is_return(type):
    return 1 if type=='RETURN' else 0

def is_order(type):
    return 1 if type=='ORDER' else 0




# Create UDF in Spark

total_cost_udf = udf(total_cost, DoubleType())
is_return_udf =  udf(is_return, IntegerType())
is_order_udf = udf(is_order, IntegerType())

# Register UDF to use with spark
spark.udf.register("total_cost_udf",total_cost_udf)
spark.udf.register("is_return_udf",is_return_udf)
spark.udf.register("is_order_udf",is_order_udf)




# Using Spark SQL for creating aggregated df

aggregated_df = spark.sql(f"""
select invoice_no, country, timestamp, 
sum(total_cost_udf(unit_price, quantity, type)) as total_cost, 
sum(quantity) as total_items,
sum(is_order_udf(type)) is_order,
sum(is_return_udf(type)) as is_return
from orders_data 
group by invoice_no, country, timestamp,type
""")




aggregated_df.printSchema()




# Stream aggregated DF every minute

console_stream = aggregated_df  \
	.writeStream  \
	.outputMode("complete") \
    .trigger(processingTime="1 minute") \
	.format("console").option("truncate", "false")

console_stream.start()





# # Create time based KPI with 1 minute tumbling window

time_based_kpi_df = messages_df.withWatermark("timestamp", "0 second") \
                .groupBy(window('timestamp','1 minute')) \
                .agg(sum(total_cost_udf('unit_price','quantity','type')).alias('total_sale_volume'),
                    count('invoice_no').alias('OPM'),
                    (sum(is_return_udf('type'))/(sum(is_return_udf('type'))+sum(is_order_udf('type')))).alias('rate_of_return'),
                    (sum(total_cost_udf('unit_price','quantity','type'))/(sum(is_return_udf('type'))+sum(is_order_udf('type')))) \
                     .alias("average_transaction_size"))
time_based_kpi_df.printSchema()




# Create time based KPI with 1 minute tumbling window

time_country_based_kpi_df = messages_df.withWatermark("timestamp", "0 second") \
                .groupBy(window('timestamp','1 minute'), 'country') \
                .agg(sum(total_cost_udf('unit_price','quantity','type')).alias('total_sale_volume'),
                    count('invoice_no').alias('OPM'),
                    (sum(is_return_udf('type'))/(sum(is_return_udf('type'))+sum(is_order_udf('type')))).alias('rate_of_return'))
time_country_based_kpi_df.printSchema()




# timebased_kpi_df\
# 	.writeStream  \
# 	.outputMode("complete") \
#     .trigger(processingTime="10 seconds") \
# 	.format("console").option("truncate", "false").start()




def store_to_json(df, path, trigger_interval):
    # for c_name, c_type in df.dtypes:
    #     if c_name!='window' and c_type in ('double', 'float'):
    #         df = df.withColumn(c_name, round(col(c_name), 2))
    json_stream = df.coalesce(1).writeStream  \
	.outputMode("append")  \
    .format("json") \
    .option("path", path) \
    .option("checkpointLocation", f"{path}/checkpoint") \
    .option("header", "true") \
    .trigger(processingTime=trigger_interval) 
    json_stream.start()




store_to_json(df=time_based_kpi_df,path="time_based_kpi",trigger_interval="10 minutes")
store_to_json(df=time_country_based_kpi_df,path="time_country_based_kpi",trigger_interval="10 minutes")

spark.streams.awaitAnyTermination()




