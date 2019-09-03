import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SaleDistributionbyProdNameType {
  def SaleDistributionbyProdNameType(spark: SparkSession): DataFrame = {
  /*def main(args: Array[String]): Unit = {*/

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder().appName("Team20").master("local[*]").getOrCreate()

    val productDf = spark.read.option("delimiter", "|").schema(Utils.productSchema)
      .csv("C:\\Users\\anand_jha2\\AnandJha\\POC\\DBSHacktronT20\\data\\input\\Product.txt").as("product")


    val salesDf = spark.read.option("delimiter", "|").schema(Utils.salesSchema)
      .csv("C:\\Users\\anand_jha2\\AnandJha\\POC\\DBSHacktronT20\\data\\input\\Sales.txt").as("sales")


    import spark.implicits._

    val saleProdDf = salesDf.join(productDf, $"sales.product_id" === $"product.product_id").
      //select($"product_name", $"product_type", $"total_amount", $"total_quantity").
      select($"sales.*", $"product.product_name", $"product.product_type").
      groupBy("product_name", "product_type").count()

    saleProdDf.show(5, false);


    //---------------------------------------------------






    /*productDf.createOrReplaceTempView("product")
    salesDf.createOrReplaceTempView("sales")

    import spark.implicits._

    println("Sales Prod DF")


    val newdf = spark.
      sql("select p.product_name, p.product_type," +
        " count(*) from sales s inner join product p on s.product_id = p.product_id group by product_name, product_type")



    println("new")
    newdf.show(false)
    saleProdDf*/
  }

}
