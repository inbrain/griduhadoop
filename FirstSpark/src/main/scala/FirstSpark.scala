import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.types._


object FirstSpark extends App {

  val sConf = new SparkConf().setAppName("FirstSpark").setMaster("local[2]")
  val sContext = new SparkContext(sConf)

  val prop = new java.util.Properties
  prop.setProperty("driver", "com.mysql.jdbc.Driver")
  prop.setProperty("user", "root")
  prop.setProperty("password", "cloudera")

  val url = "jdbc:mysql://localhost:3306/sparkcourse"

  val sqlContext = new SQLContext(sContext)
  val salesDF = {
    val schema = StructType(Array(
      StructField("name", StringType, false),
      StructField("price", IntegerType, false),
      StructField("cat", StringType, false),
      StructField("ip", StringType, false),
      StructField("date", StringType, false)))

    sqlContext.read.format("com.databricks.spark.csv").
      option("header", "false").
      schema(schema).load("hdfs://quickstart.cloudera:8020/user/cloudera/flumex/2000/01/*/F*")
  }

  val blocksDf = {
    val schema = StructType(Array(
      StructField("network", StringType, true),
      StructField("geoname_id", IntegerType, true),
      StructField("registered_country_geoname_id", IntegerType, true),
      StructField("represented_country_geoname_id", IntegerType, true),
      StructField("is_anonymous_proxy", IntegerType, true),
      StructField("is_satellite_provider", IntegerType, true)
    ))

    sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").
      schema(schema).load("hdfs://quickstart.cloudera:8020/user/cloudera/geo/blocks")
  }

  val locationsDf = {
    val schema = StructType(Array(
      StructField("geoname_id", IntegerType, true),
      StructField("locale_code", StringType, true),
      StructField("continent_code", StringType, true),
      StructField("continent_name", StringType, true),
      StructField("country_iso_code", StringType, true),
      StructField("country_name", StringType, true),
      StructField("is_in_european_union", IntegerType, true)
    ))

    sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").
      schema(schema).load("hdfs://quickstart.cloudera:8020/user/cloudera/geo/locations")
  }

  val joinUdf = org.apache.spark.sql.functions.udf((subnet:String, ip:String) => {new SubnetUtils(subnet).getInfo().isInRange(ip)})
  val blockWithLocation = locationsDf.join(blocksDf, Seq("geoname_id"))
  val salesByCountry = blockWithLocation.join(salesDF, joinUdf(blockWithLocation("network"), salesDF("ip")))

  salesDF.registerTempTable("maindata")
  locationsDf.registerTempTable("locations")
  blockWithLocation.registerTempTable("blockswithlocations")
  salesByCountry.registerTempTable("salesbycountry")

  //Select top 10  most frequently purchased categories
  sqlContext.sql("select cat, x from (select cat, count(*) as x from maindata group by cat) as j order by x desc limit 10").
    write.mode(SaveMode.Overwrite).jdbc(url, "topcat", prop)
  //Select top 10 most frequently purchased product in each category
  sqlContext.sql("select distinct cat, name, x from (select cat, name, count(name) as x from maindata group by cat, name) as j order by x desc limit 10").
    write.mode(SaveMode.Overwrite).jdbc(url, "topproducts", prop)
  //Select top 10 countries with the highest money spending
  sqlContext.sql("select country_name, sum(price) as x from salesbycountry group by country_name order by x desc limit 10").
    write.mode(SaveMode.Overwrite).jdbc(url, "bestsellingcountries", prop)


}
