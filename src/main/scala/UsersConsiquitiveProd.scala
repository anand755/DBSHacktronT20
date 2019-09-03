import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object UsersConsiquitiveProd {

  def usersConsiquitiveProd(spark: SparkSession): DataFrame = {

    val customerDf = spark.read.option("delimiter", "|").schema(Utils.customerSchema)
      .csv(Utils.productPath).as("product")

    val salesDf = spark.read.option("delimiter", "|").schema(Utils.salesSchema)
      .csv(Utils.salesPath).as("sales")

    val resultDF = customerDf.join(salesDf)
    resultDF
  }
}
