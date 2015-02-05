package nibbler.evaluation

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class ErrorCalculationFunction extends Serializable {

  private def computeError(row: (Double, Double)) = {
    Math.log(Math.abs(row._1 - row._2) + 1.0)
  }

  private def add(left: Double, right: Double) = {
    left + right
  }

  def calculateError(symbolicallyDifferentiated: RDD[(Double, Double)]): Double = {
    val sumOfRowErrors = symbolicallyDifferentiated
      .map(computeError)
      .reduce(add)

    -sumOfRowErrors
  }

}
