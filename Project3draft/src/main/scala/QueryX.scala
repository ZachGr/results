import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

import java.io.File
import scala.io.Source

object QueryX {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    //Logger.getLogger("org").setLevel(Level.ERROR)
    println("Created spark session.")
    //empty data frames
    var dfe1 = spark.emptyDataFrame
    var dfe2 = spark.emptyDataFrame
    var dfe3 = spark.emptyDataFrame

    val data = Seq(Row("Total 2000"))
    val schema = new StructType()
      .add("Year",StringType)
    val df2000 = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    dfe1 = df2000

    val data2 = Seq(Row("Total 2010"))
    val schema2 = new StructType()
      .add("Data2010",StringType)
    val df2010 = spark.createDataFrame(spark.sparkContext.parallelize(data2),schema2)
    dfe2 = df2010

    val data3 = Seq(Row("Total 2020"))
    val schema3 = new StructType()
      .add("Data2020",StringType)
    val df2020 = spark.createDataFrame(spark.sparkContext.parallelize(data3),schema3)
    dfe3 = df2020

    val testing1 = spark.read.format("csv").option("header","true").load("C:\\Users\\Fenix Xia\\Documents\\GitHub\\results\\Project3draft\\Combine2000RG.csv") // File location in hdfs
    testing1.createOrReplaceTempView("Testing1Imp")

    val testing2 = spark.read.format("csv").option("header","true").load("C:\\Users\\Fenix Xia\\Documents\\GitHub\\results\\Project3draft\\Combine2010RG.csv") // File location in hdfs
    testing2.createOrReplaceTempView("Testing2Imp")

    val testing3 = spark.read.format("csv").option("header","true").load("C:\\Users\\Fenix Xia\\Documents\\GitHub\\results\\Project3draft\\Combine2020RG.csv") // File location in hdfs
    testing3.createOrReplaceTempView("Testing3Imp")

    val headers = spark.read.format("csv").option("header","true").load("C:\\Users\\Fenix Xia\\Documents\\GitHub\\results\\Project3draft\\headers.csv") // File location in hdfs
    headers.createOrReplaceTempView("HeaderImp")
    spark.sql("Select * from HeaderImp").show()

    val t1 = System.nanoTime

    var testingS = testing1.drop("FILEID", "STUSAB", "Region", "Division", "CHARITER", "CIFSN", "LOGRECNO")

    var ColumnNames = testingS.columns
    println(ColumnNames.mkString)
    var Columnstring = ColumnNames.mkString("sum(", "),sum(", ")")
    println(Columnstring)
    var Columnstring2 = ColumnNames.mkString(",")
    var Columnlist = Columnstring.split(",")
    println(Columnlist.mkString)

    var HeaderNames = headers.columns
    var Headerstring = HeaderNames.mkString(",")
    var Headerlist = Headerstring.split(",")

    var newdata1 = spark.sql(s"SELECT $Columnstring FROM Testing1Imp").toDF()
    var newdata2 = spark.sql(s"SELECT $Columnstring FROM Testing2Imp").toDF()
    var newdata3 = spark.sql(s"SELECT $Columnstring FROM Testing3Imp").toDF()
    //2000
    dfe1 = dfe1.join(newdata1)
    //2010
    dfe2 = dfe2.join(newdata2)
    //2020
    dfe3 = dfe3.join(newdata3)

    var Join1 = dfe1.union(dfe2)

    var Join2 = Join1.union(dfe3)


    var lastimp = Columnlist.length

    for ( i <- 0 until lastimp){

      var FinalTable = Join2.withColumnRenamed(s"${Columnlist(i)}",f"${Headerlist(i).dropRight(1)}")
      Join2 = FinalTable

    }

    Join2.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./OutputCSV2/")

    //file.outputcsv("QueryX", Join2)

    val duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration/1000000000) + " Seconds")
  }
}
