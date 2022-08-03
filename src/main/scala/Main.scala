import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DecimalType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}

import Console.{CYAN, GREEN, RED, RESET, WHITE, YELLOW}
import org.apache.spark.sql
import os.truncate

import scala.io.StdIn.readLine

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project3")
    var df = session.spark.read.option("header", "true").csv("Combine2020RG.csv")
    var df2 = session.spark.read.option("header", "true").csv("Combine2010RG.csv")
    var df3 = session.spark.read.option("header", "true").csv("Combine2000RG.csv")
    df = df.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df2 = df2.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df3 = df3.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))

    //df3 = df3.withColumn("whitealone", col("whitealone").cast(DecimalType(18, 1)))
    df.createOrReplaceTempView("c2020")
    df2.createOrReplaceTempView("c2010")
    df3.createOrReplaceTempView("c2000")
    //df.withColumn("total", col("total").cast(DecimalType(18, 1)))
    //session.spark.sql("SELECT * [except total] FROM c2020").show()

    session.spark.sql("SELECT * FROM c2020").show()
    session.spark.sql("SELECT * FROM c2010").show()
    session.spark.sql("SELECT * FROM c2000").show()


    session.spark.sql("SELECT CAST(pop2000 AS String), pop2010, pop2020 FROM "+
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020) "+
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010)"+
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000)").show()

    /****************************CHANGING POP IN STATES OVER DECADES*******************************************************/
    session.spark.sql("SELECT t1.STUSAB, pop2000, pop2010, ROUND((((pop2010 - pop2000)/pop2000)*100),1) AS pop2000_2010, " +
                      "pop2020, ROUND((((pop2020 - pop2010)/pop2010)*100),1) AS pop2010_2020 FROM "+
                      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 "+
                      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 "+
                      "ON t1.STUSAB = t2.STUSAB " +
                      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
                      "ON t1.STUSAB = t3.STUSAB").show()
    /**********************************************************************************/


    /*******************************TRENDLINE PREDICITON***************************************************/
    session.spark.sql("SELECT t1.STUSAB, pop2000, pop2010, "+
      "ROUND(((((pop2010 - pop2000)/pop2000) * pop2010) + pop2010),1) AS pop2020Pred, pop2020 FROM "+
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 "+
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 "+
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB").show()


    /********************************************************************************************************/

    /*******************************FASTEST GROWING STATE***********************************************/
    session.spark.sql("SELECT t1.STUSAB, pop2020, pop2010, pop2000, "+
      "ROUND((((pop2020 - pop2000)/pop2000) * 100),1) AS popGrowth FROM "+
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 "+
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 "+
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB ORDER BY popGrowth DESC").show()


    session.spark.sql("SELECT t1.STUSAB, pop2020, pop2010, pop2000, "+
      "ROUND((((pop2020 - pop2000)/pop2000) * 100),1) AS popGrowth FROM "+
      "(SELECT STUSAB, p0010001 AS pop2020 FROM c2020) AS t1 "+
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2010 FROM c2010) AS t2 "+
      "ON t1.STUSAB = t2.STUSAB " +
      "INNER JOIN (SELECT STUSAB, p0010001 AS pop2000 FROM c2000) AS t3 " +
      "ON t1.STUSAB = t3.STUSAB AND ROUND((((pop2020 - pop2000)/pop2000) * 100),1) < 0 ORDER BY popGrowth ASC").show()
    //population prediction function
    //session.spark.sql("SELECT SUM() FROM yo WHERE label LIKE '%Hi%' AND NOT label LIKE '%Not%'").show()*/
  }

}
