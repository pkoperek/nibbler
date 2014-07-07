/**
 * User: koperek
 * Date: 06.07.14
 * Time: 15:32
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main {

  val applicationName = "TestApp"

  def main(args: Array[String]) {

    validate(args)

    val masterUrl = args(0)
    val inputFilePath = args(1)
    val conf = new SparkConf().setAppName(applicationName).setMaster(masterUrl)
    val sparkContext = new SparkContext(conf)

    val inputFile: RDD[String] = sparkContext.textFile(inputFilePath)
    val timestampValues = inputFile.map(splitToTimestampAndValue)

    println("Evaluation result: " + evaluate(timestampValues))
  }

  private def splitToTimestampAndValue(input: String): (Long, Double) = {
    val splittedStrings = input.split(",")
    val timestampAsString = splittedStrings(0)

    return (1L, splittedStrings(1).toDouble)
  }

  private def evaluate(timestampValues: RDD[(Long, Double)]): Double = {
    val evaluationService = new EvaluationService(timestampValues)

    evaluationService.evaluate({
      x => x * 2
    })
  }

  def validate(args: Array[String]) = {
    if (args.length != 2) {
      println("Specify master URL and data files as parameters! Eg. mesos://149.156.10.32:1237 hdfs://master/data/data.csv")
      System.exit(1)
    }
  }
}