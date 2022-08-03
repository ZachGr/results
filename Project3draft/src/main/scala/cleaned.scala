import org.apache.spark.sql.functions.{col, format_number, lit}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

object cleaned extends App{
  val session = new SparkInit("Project3")
  var df1 = session.spark.read.option("header", "true").csv("Combine2000RG.csv")
  var df2 = session.spark.read.option("header", "true").csv("Combine2010RG.csv")
  var df3 = session.spark.read.option("header", "true").csv("Combine2020RG.csv")
  var df4 = session.spark.read.option("header", "true").csv("headers.csv")
  df1.createOrReplaceTempView("c2000")
  df2.createOrReplaceTempView("c2010")
  df3.createOrReplaceTempView("c2020")
  //slicing the RG files column names
  var dc1 = df1.columns.slice(7,151).toList
  var dc2 = df2.columns.slice(7,151).toList
  var dc3 = df3.columns.slice(7,151).toList
  //column names actual
  var dc4 = df4.columns.toList
  //maps
  var dm1 = (dc1 zip dc4).toMap
  var dm2 = (dc2 zip dc4).toMap
  var dm3 = (dc3 zip dc4).toMap
  //empty data frames
  var dfe1 = session.spark.emptyDataFrame
  var dfe2 = session.spark.emptyDataFrame
  var dfe3 = session.spark.emptyDataFrame
  //casting to int
  for(i <- dc1){
    df1 = df1.withColumn(s"$i", col(s"$i").cast(DecimalType(38,0)))
  }
  for(i <- dc2){
    df2 = df2.withColumn(s"$i", col(s"$i").cast(DecimalType(38,0)))
  }
  for(i <- dc3){
    df3 = df3.withColumn(s"$i", col(s"$i").cast(DecimalType(38,0)))
  }
  //reading data for tables

  val data = Seq(Row("Total 2000"))
  val schema = new StructType()
    .add("Year",StringType)
  val df2000 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data),schema)
  dfe1 = df2000

  for(i <- dc1){
    val data = session.spark.sql(s"Select SUM($i) as "+ dm1.get(i).get +" from c2000").toDF()
    //data.withColumn("data1", lit("totalx"))
    //data.createOrReplaceTempView("data")
    //session.spark.sql("Select * from data").show()
    //println(data.head.get(0))
    dfe1 = dfe1.withColumn(dm1.get(i).get, lit(data.head.get(0)))

  }

  dfe1.show()
  println("end of 2000 tables")
  //data2010
  val data2 = Seq(Row("Total 2010"))
  val schema2 = new StructType()
    .add("Data2010",StringType)
  val df2010 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data2),schema2)
  dfe2 = df2010
  for(i <- dc2){
    val data = session.spark.sql(s"Select SUM($i) as "+ dm2.get(i).get +" from c2010").toDF()
    dfe2 = dfe2.withColumn(dm2.get(i).get, lit(data.head().get(0)))
  }
  dfe2.show()
  println("end of 2010 tables")

  val data3 = Seq(Row("Total 2020"))
  val schema3 = new StructType()
    .add("Data2020",StringType)
  val df2020 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data3),schema3)
  dfe3 = df2020
  for(i <- dc3){
    val data = session.spark.sql(s"Select SUM($i) as "+ dm3.get(i).get +" from c2020").toDF
    dfe3 = dfe3.withColumn(dm3.get(i).get, lit(data.head().get(0)))
  }
  dfe3.show()
  println("end of 2020 tables")
  println(dc3.mkString(","))
  println(dc2.mkString(","))
  println(dc1.mkString(","))
  println(dc4.mkString(","))
  var dfeUlt1 = dfe1.union(dfe2)
  val dfeUlt2 = dfeUlt1.union(dfe3)
  dfeUlt2.show()
  dfeUlt2.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/raceqquery/")
}//end of app
