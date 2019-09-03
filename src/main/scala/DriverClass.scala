import org.apache.spark.sql.{DataFrame, SparkSession}

object DriverClass {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Team20").master("local[*]").getOrCreate()

    val notSoldProd = NotSoldProduct.notSoldProduct(spark)
    notSoldProd.show(false)

    val saleDistByProdNameTyp = SaleDistributionbyProdNameType.SaleDistributionbyProdNameType(spark)
    saleDistByProdNameTyp.show(false)

    val totAmountResDf = TotAmount.totAmount(spark)
    totAmountResDf.show(20, false)

    val userConDf = UsersConsiquitiveProd.usersConsiquitiveProd(spark)
    userConDf.show(10, false)


  }
}
