package nibbler

/**
 * User: koperek
 * Date: 06.07.14
 * Time: 15:32
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Main {

  def main(args: Array[String]) {

    validate(args)

    val masterUrl = args(0)
    val inputFilePath = args(1)
    val conf = new SparkConf()
      .setAppName("TestApp")
      .set("spark.executor.uri", "http://d3kbcqa49mib13.cloudfront.net/spark-1.0.0-bin-hadoop2.tgz")
      .setMaster(masterUrl)

    val sparkContext = new SparkContext(conf)

    val inputFile: RDD[String] = sparkContext.textFile(inputFilePath)
    println("Lines count: " + inputFile.count())

    //    val timestampValues = inputFile.map(splitToTimestampAndValue)
    //
    //    println("Evaluation result: " + evaluate(timestampValues))

  }

  private def splitToTimestampAndValue(input: String): (Long, Double) = {
    val splittedStrings = input.split(",")
    val timestampAsString = splittedStrings(0)

    return (1L, splittedStrings(1).toDouble)
  }

  def validate(args: Array[String]) = {
    if (args.length != 2) {
      println("Specify master URL and data files as parameters! Eg. mesos://149.156.10.32:1237 hdfs://master/data/data.csv")
      System.exit(1)
    }
  }
}
