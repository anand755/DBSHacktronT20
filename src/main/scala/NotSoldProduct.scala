import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object NotSoldProduct {

  def notSoldProduct(spark: SparkSession): DataFrame = {

    val productDf = spark.read.option("delimiter", "|").schema(Utils.productSchema)
      .csv(Utils.productPath).as("product")

    val salesDf = spark.read.option("delimiter", "|").schema(Utils.salesSchema)
      .csv(Utils.salesPath).as("sales")

    productDf.createOrReplaceTempView("product")
    salesDf.createOrReplaceTempView("sales")

    val resultDf = spark.
      sql("select p.* from product p left outer join sales s on p.product_id = s.product_id where s.product_id is null")

    resultDf
  }

}
