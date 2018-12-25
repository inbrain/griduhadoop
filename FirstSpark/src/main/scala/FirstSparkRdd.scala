import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}


case class AppConf(dbUser: String, dbPass: String, dbUrl: String, salesFilePath: String, blockFilePath: String, locationFilePath: String)

object AppConf {
  def apply(config: Config): AppConf = {
    AppConf(
      config.getString("db.user"),
      config.getString("db.pass"),
      config.getString("db.url"),
      config.getString("sales.path"),
      config.getString("geo.blockPath"),
      config.getString("geo.locationPath"),
    )
  }
}

object FirstSparkRdd extends App {

  val appConf = AppConf(ConfigFactory.load().getConfig("course"))

  val sConf = new SparkConf().setAppName("FirstSpark").setMaster("local[2]")
  val sContext = new SparkContext(sConf)


  val prop = new java.util.Properties
  prop.setProperty("driver", "com.mysql.jdbc.Driver")
  prop.setProperty("user", appConf.dbUser)
  prop.setProperty("password", appConf.dbPass)

  val sqlContext = new SQLContext(sContext)

  val salesRdd = sContext.textFile(appConf.salesFilePath).map(_.split(","))

  val topCatDf = {
    val salesByCat = salesRdd.map(arr => (arr(2), arr))

    val topCatCol = salesByCat.map(x => (x._1, 1)).
      reduceByKey(_ + _).
      sortBy(_._2, false).
      map(x => Row(x._1, x._2)).
      take(10)

    val shema = StructType(
      List(
        StructField("cat", DataTypes.StringType),
        StructField("sold", DataTypes.IntegerType)
      )
    )

    sqlContext.createDataFrame(sContext.parallelize(topCatCol), shema)
  }

  case class CatProd(cat: String, prod: String)

  case class CatSell(cat: String, sold: Int)

  case class ProdSell(prod: String, sold: Int)

  implicit val catSellOrdering: Ordering[CatSell] = new Ordering[CatSell] {
    override def compare(x: CatSell, y: CatSell): Int = {
      if (x.cat == y.cat) x.sold.compareTo(y.sold)
      else x.cat.compare(y.cat)
    }
  }

  val topProdInCatDf = {
    val salesByCatProd = salesRdd.map(arr => (CatProd(arr(2), arr(0)), arr))

    val topCatProdRdd = salesByCatProd.map(x => (x._1, 1)).
      reduceByKey(_ + _).
      sortBy(x => CatSell(x._1.cat, x._2), false, 1).
      map(x => (x._1.cat, List(ProdSell(x._1.prod, x._2)))).
      reduceByKey((acc, newOne) => if (acc.size == 10) acc else acc ++ newOne).
      flatMap {
        case (key, value) => value.map(v => (key, v))
      }.
      map(x => Row(x._1, x._2.prod, x._2.sold))

    val shema = StructType(
      List(
        StructField("cat", DataTypes.StringType),
        StructField("prod", DataTypes.StringType),
        StructField("sold", DataTypes.IntegerType)
      )
    )

    sqlContext.createDataFrame(topCatProdRdd, shema)
  }

  val topCountryDf = {
    val blockRdd = sContext.textFile(appConf.blockFilePath)
    val blockHeader = blockRdd.first
    val rectifiedBlockRdd = blockRdd.filter(_ != blockHeader).map(_.split(","))
      .filter(l => !l(0).isEmpty && !l(1).isEmpty).
      map(x => (x(1).toLong, x(0)))

    val locationRdd = sContext.textFile(appConf.locationFilePath)
    val locationHeader = locationRdd.first
    val rectifiedLocationRdd = locationRdd.filter(_ != locationHeader).map(_.split(","))
      .filter(l => !l(0).isEmpty && !l(5).isEmpty).
      map(x => (x(0).toLong, x(5)))

    val countryNetworkRdd = rectifiedLocationRdd.join(rectifiedBlockRdd).map(x => (x._2._1, x._2._2))

    val ipPriceRdd = salesRdd.map(arr => (arr(3), arr(1).toInt))

    val best10Countries = ipPriceRdd.cartesian(countryNetworkRdd).
      collect {
        case ((ip, price), (country, net)) if !ip.isEmpty && !net.isEmpty && new SubnetUtils(net).getInfo.isInRange(ip) =>
          (country, price)
      }.
      reduceByKey(_ + _).
      sortBy(_._2, false, 1).
      map(x => Row(x._1, x._2)).
      take(10)


    val schema = StructType(
      List(
        StructField("country", DataTypes.StringType),
        StructField("totalprice", DataTypes.IntegerType)
      )
    )

    sqlContext.createDataFrame(sContext.parallelize(best10Countries), schema)
  }


    topCatDf.write.
      mode(SaveMode.Overwrite).
      jdbc(appConf.dbUrl, "topcat", prop)

   topProdInCatDf.write.
    mode(SaveMode.Overwrite).
    jdbc(appConf.dbUrl, "topproducts", prop)

  topCountryDf.write.
    mode(SaveMode.Overwrite).
    jdbc(appConf.dbUrl, "bestsellingcountries", prop)
}
