import org.apache.spark.sql.functions.{col, format_number}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}

object cleaned extends App{
  val session = new SparkInit("Project3")
  var df1 = session.spark.read.option("header", "true").csv("Combine2000RG.csv")
  var df2 = session.spark.read.option("header", "true").csv("Combine2010RG.csv")
  var df3 = session.spark.read.option("header", "true").csv("Combine2020RG.csv")
  var df4 = session.spark.read.option("header", "true").csv("headers.csv")
  df1.createOrReplaceTempView("c2000")
  df2.createOrReplaceTempView("c2010")
  df3.createOrReplaceTempView("c2020")
  //slicing the RG files
  var dc1 = df1.columns.slice(7,151).toList
  var dc2 = df2.columns.slice(7,151).toList
  var dc3 = df3.columns.slice(7,151).toList
  //column names actual
  var dc4 = df4.columns.toList
  //maps
  var dm1 = (dc1 zip dc4).toMap
  var dm2 = (dc2 zip dc4).toMap
  var dm3 = (dc3 zip dc4).toMap
  println(dm3.get("P0010047"))
  //casting to int
  for(i <- dc3){
    df3 = df3.withColumn(s"$i", col(s"$i").cast(DecimalType(38,0)))
  }
  //reading data
  //+ dm3.get(i).get +
  for(i <- dc3){
    session.spark.sql(s"Select SUM($i) as "+ dm3.get(i).get +" from c2020").show(1,false)
  }
  println(dc3.mkString(","))
  println(dc2.mkString(","))
  println(dc1.mkString(","))
  println(dc4.mkString(","))

}//end of app
