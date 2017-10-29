package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def parsePoint(pointString:String): Array[Double] = {
    val coordinates = Array(0.0, 0.0)
    val pointCoor = pointString.split(",")
    if (pointCoor.length < 2) return coordinates
    for ((str, index) <- pointCoor.zipWithIndex) {
      coordinates(index) = str.toDouble
    }
    return coordinates
  }
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    if (queryRectangle == null || pointString == null || queryRectangle.length < 0 || pointString.length < 0) return false
    val recCoordinates = queryRectangle.split(",")
    if (recCoordinates.length < 4) return false
    val coordinates = Array[Double](4)
    for ((str, index) <- recCoordinates.zipWithIndex) {
      coordinates(index) = str.toDouble
    }
    val recX1:Double = Math.min(coordinates(0), coordinates(2))
    val recY1:Double = Math.min(coordinates(1), coordinates(3))
    val recX2:Double = Math.max(coordinates(0), coordinates(2))
    val recY2:Double = Math.max(coordinates(1), coordinates(3))
    val pointCoordinates = pointString.split(",")
    if (pointCoordinates.length < 2) return false
    val pointX1:Double = pointCoordinates(0).toDouble
    val pointY1:Double = pointCoordinates(1).toDouble
    if (pointX1 < recX1 || pointX1 > recX2 || pointY1 < recY1 || pointY1 > recY2) return false
    return true
  }
  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    if (pointString1 == null || pointString1.length < 0 || pointString2 == null || pointString2.length < 0
    || distance < 0.0d) return false
    val point1Coordinates = parsePoint(pointString1)
    val point2Coordinates = parsePoint(pointString2)
    val euclidean: Double = Math.sqrt(Math.pow(point1Coordinates(0) - point2Coordinates(0), 2) + Math.pow(point1Coordinates(1) - point2Coordinates(1), 2))
    return euclidean <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>ST_Contains(queryRectangle, pointString))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>ST_Contains(queryRectangle, pointString))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>ST_Within(pointString1, pointString2, distance))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>ST_Within(pointString1, pointString2, distance))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
