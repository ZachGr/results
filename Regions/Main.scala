import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DecimalType

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project3")
    var df = session.spark.read.option("header", "true").csv("Combine2020RG.csv")
    var df2 = session.spark.read.option("header", "true").csv("Combine2010RG.csv")
    var df3 = session.spark.read.option("header", "true").csv("Combine2000RG.csv")
    df = df.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df2 = df2.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df3 = df3.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df.createOrReplaceTempView("c2020")
    df2.createOrReplaceTempView("c2010")
    df3.createOrReplaceTempView("c2000")
    //df.withColumn("total", col("total").cast(DecimalType(18, 1)))

    session.spark.sql("SELECT * FROM c2020") //.show()
    session.spark.sql("SELECT * FROM c2010") //.show()
    session.spark.sql("SELECT * FROM c2000") //.show()


    //REGION WITH THE HIGHEST POPULATION
    //Aggregate the total population in the five US regions: the Northeast, Southwest, West, Southeast, and Midwest

    //NORTHEAST
    var dfne = session.spark.sql("SELECT pop2000, pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020 WHERE Region = 'Northeast') " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010 WHERE Region = 'Northeast') " +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000 WHERE Region = 'Northeast')")
    //convert dataframe to a dataframe with a new column called "Region"
    var dfne2 = dfne.withColumn("Region", lit("Northeast"))
    dfne2.show()

    //SOUTHWEST using only West South Central
    var dfsw = session.spark.sql("SELECT pop2000, pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020 WHERE Division = 'West_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010 WHERE Division = 'West_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000 WHERE Division = 'West_South_Central')")
    //convert dfsw to a dataframe with a new column called "Region"
    var dfsw2 = dfsw.withColumn("Region", lit("Southwest"))
    dfsw2.show()

    //WEST
    var dfw = session.spark.sql("SELECT pop2000, pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020 WHERE Region = 'West') " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010 WHERE Region = 'West') " +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000 WHERE Region = 'West')")
    //convert dfw to a dataframe with a new column called "Region"
    var dfw2 = dfw.withColumn("Region", lit("West"))
    dfw2.show()

    //SOUTHEAST using East South Central and South Atlantic
    var dfse = session.spark.sql("SELECT pop2000, pop2010, pop2020 FROM" +
      "(SELECT pop2020A + pop2020B AS pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020A FROM c2020 WHERE Division = 'East_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2020B FROM c2020 WHERE Division = 'South_Atlantic')) " +

      "join (SELECT pop2010A + pop2010B AS pop2010 FROM " +
      "(SELECT SUM(p0010001) AS pop2010A FROM c2010 WHERE Division = 'East_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2010B FROM c2010 WHERE Division = 'South_Atlantic'))" +

      "join (SELECT pop2000A + pop2000B AS pop2000 FROM " +
      "(SELECT SUM(p0010001) AS pop2000A FROM c2000 WHERE Division = 'East_South_Central') " +
      "join (SELECT SUM(p0010001) AS pop2000B FROM c2000 WHERE Division = 'South_Atlantic'))")
    //convert dfse to a dataframe with a new column called "Region"
    var dfse2 = dfse.withColumn("Region", lit("Southeast"))
    dfse2.show()

    //MIDWEST
    var dfmw = session.spark.sql("SELECT pop2000, pop2010, pop2020 FROM " +
      "(SELECT SUM(p0010001) AS pop2020 FROM c2020 WHERE Region = 'Midwest') " +
      "join (SELECT SUM(p0010001) AS pop2010 FROM c2010 WHERE Region = 'Midwest') " +
      "join (SELECT SUM(p0010001) AS pop2000 FROM c2000 WHERE Region = 'Midwest')")
    //convert dfmw to a dataframe with a new column called "Region"
    var dfmw2 = dfmw.withColumn("Region", lit("Midwest"))
    dfmw2.show()

    //SHOW ALL
    var total = dfne2.union(dfsw2).union(dfw2).union(dfse2).union(dfmw2)
    total.select("Region", "pop2000", "pop2010", "pop2020").show()

    
  }
}








