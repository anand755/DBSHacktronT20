from pyspark.sql import SparkSession                                                                                                                                                       
from pyspark.sql.types import StructType, StructField, StringType, IntegerType                                                                                                             
cofinan
spark=SparkSession.builder.appName("second-most-purchase.py").enableHiveSupport().getOrCreate()                                                                                            
cofinan
cust_columns=[StructField("customer_id", StringType(), True), StructField("customer_first_name", StringType(), True), StructField("customer_last_name",StringType(),True),StructField("phon
e_number",StringType(),True)]                                                                                                                                                              
cust_schema=StructType(cust_columns)                                                                                                                                                       
cofinan
cust_df=spark.read.option("delimiter","|").schema(cust_schema).csv("dbs/data/customer.txt")                                                                                                
cust_df.createOrReplaceTempView("cust_tbl")                                                                                                                                                
cofinan
sales_columns=[StructField("transaction_id", StringType(), True), StructField("customer_id", StringType(), True), StructField("product_id",StringType(),True),StructField("timestamp",Strin
gType(),True),StructField("total_amount",StringType(), True), StructField("total_quantity", IntegerType(), True)]                                                                          
sales_schema=StructType(sales_columns)                                                                                                                                                     
sales_df=spark.read.option("delimiter","|").schema(sales_schema).csv("dbs/data/sales.txt")                                                                                                 
sales_df.createOrReplaceTempView("sales_tbl")                                                                                                                                              
cofinan
refund_columns=[StructField("refund_id", StringType(), True), StructField("original_transaction_id", StringType(), True), StructField("customer_id",StringType(),True),StructField("product
_id",StringType(),True),StructField("timestamp",StringType(), True), StructField("refund_amount", StringType(), True),StructField("refund_quantity", IntegerType(), True)]                 
refund_schema=StructType(refund_columns)                                                                                                                                                   
refund_df=spark.read.option("delimiter","|").schema(refund_schema).csv("dbs/data/refund.txt")                                                                                              
refund_df.createOrReplaceTempView("refund_tbl")                                                                                                                                            
cofinan
product_columns=[StructField("product_id", StringType(), True), StructField("product_name", StringType(), True), StructField("product_type",StringType(),True),StructField("product_version
",StringType(),True),StructField("product_price",StringType(), True)]                                                                                                                      
product_schema=StructType(product_columns)                                                                                                                                                 
product_df=spark.read.option("delimiter","|").schema(product_schema).csv("dbs/data/product.txt")                                                                                           
product_df.createOrReplaceTempView("product_tbl")                                                                                                                                          
cofinan
##########Sales dist task                                                                                                                                                                  
sales_dist_df=spark.sql("select st.product_id, product_type, sum(st.total_quantity) from sales_tbl st join product_tbl pt on st.product_id=pt.product_id group by st.product_id,product_typ
e")                                                                                                                                                                                        
cofinan
sales_dist_df.coalesce(1).write.format("json").save("dbs/data/sales_dist")                                                                                                                 
cofinan
##########Second most user                                                                                                                                                                 
sec_most_df=spark.sql("select customer_id from (select customer_id, Dense_rank() over(order by sum(total_quantity) desc) as id from sales_tbl where substring(timestamp,7,4)='2013' and sub
string(timestamp,4,2)='05' group by customer_id) a where a.id='2'")                                                                                                                        
sec_most_df.createOrReplaceTempView("sec_most_tbl")                                                                                                                                        
cust_name_df=spark.sql("select customer_first_name from cust_tbl ct join sec_most_tbl smt on ct.customer_id=smt.customer_id")                                                              
cust_name_df.write.format("text").save("dbs/data/sec_most")

                                                                                                                                                                           
cofinan
########no product id sale task                                                                                                                                                            
product_task_df=spark.sql("select p.* from product_tbl p left outer join sales_tbl s on p.product_id = s.product_id where s.product_id is null")                                           
product_task_df.write.format("json").save("dbs/data/shifali")
