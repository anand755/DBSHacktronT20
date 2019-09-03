import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Utils {

  val productPath = "dbs/data/product.txt"
  val salesPath = "C:\\Users\\anand_jha2\\AnandJha\\POC\\DBSHacktronT20\\data\\input\\Sales.txt"
  val customerPath = "dbs/data/customer.txt"
  val refundPath = "C:\\Users\\anand_jha2\\AnandJha\\POC\\DBSHacktronT20\\data\\input\\Refund.txt"

  val customerSchema = StructType(Array
  (StructField("customer_id", IntegerType, true),
    StructField("customer_first_name", StringType, true),
    StructField("customer_last_name", StringType, true),
    StructField("phone_number", StringType, true)
  )
  )

  val salesSchema = StructType(Array
  (StructField("transaction_id", IntegerType, true),
    StructField("customer_id", IntegerType, true),
    StructField("product_id", IntegerType, true),
    StructField("timestamp", StringType, true),
    StructField("total_amount", StringType, true),
    StructField("total_quantity", IntegerType, true)
  )
  )

  val productSchema = StructType(Array
  (StructField("product_id", IntegerType, true),
    StructField("product_name", StringType, true),
    StructField("product_type", StringType, true),
    StructField("product_version", StringType, true),
    StructField("product_price", StringType, true)
  )
  )

  val refundSchema = StructType(Array
  (StructField("refund_id", IntegerType, true),
    StructField("original_transaction_id", IntegerType, true),
    StructField("customer_id", IntegerType, true),
    StructField("product_id", IntegerType, true),
    StructField("timestamp", IntegerType, true),
    StructField("refund_amount", StringType, true),
    StructField("refund_quantity", StringType, true)
  )
  )

}
