import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TotAmount {

  /*def main(args: Array[String]): Unit = {*/
  def totAmount(spark: SparkSession): DataFrame = {
    //val spark = SparkSession.builder().appName("Team20").master("local[*]").getOrCreate()


    val refundDf = spark.read.option("delimiter", "|").schema(Utils.refundSchema)
      .csv(Utils.refundPath).as("refund")

    val salesDf = spark.read.option("delimiter", "|").schema(Utils.salesSchema)
      .csv(Utils.salesPath).as("sales")


    import spark.implicits._
    val derived_SalesDF = salesDf.
      select($"transaction_id", $"customer_id", $"product_id", substring(trim($"timestamp"), 1, 10).as("date"), substring(trim($"timestamp"), 7, 4).as("year"), $"timestamp", $"total_amount", translate($"total_amount", "$", "") as ("total_amt") cast "Int", $"total_quantity" cast "Int")

    derived_SalesDF.show(false)

    derived_SalesDF.createOrReplaceTempView("sales")
    refundDf.createOrReplaceTempView("refund")

    val resultDf1 = spark.
      sql("select s.year, sum(s.total_amt) as t_amount from sales s left outer join refund r on s.transaction_id = r.original_transaction_id where r.original_transaction_id is null and s.year = '2013' group by s.year")

    resultDf1.show(false)
    println("sdxasdigdgadgajdjagka")
    val resultDf2 = spark.
      sql("select inr.year, inr.t_amount from (select s.year, sum(s.total_amt) as t_amount from sales s left outer join refund r on trim(s.transaction_id) = trim(r.original_transaction_id) where trim(r.original_transaction_id) is null group by s.year) inr where trim(inr.year) = '2013'")


    println("result")
    resultDf2.show(false)
    resultDf2
  }

}
