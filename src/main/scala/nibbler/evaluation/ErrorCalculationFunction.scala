package nibbler.evaluation

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class ErrorCalculationFunction extends Serializable {

  private def computeError(row: (Double, Double)) = {
    Math.log(Math.abs(row._1 - row._2) + 1.0)
  }

  private def add(left: (Long, Double), right: (Long, Double)) = {
    (left._1 + right._1, left._2 + right._2)
  }

  def calculateError(symbolicallyDifferentiated: RDD[(Long, Double)], numericallyDifferentiated: RDD[(Long, Double)]): Double = {
    val sumOfRowErrors = numericallyDifferentiated.join(symbolicallyDifferentiated)
      .mapValues(computeError)
      .reduce(add)

    -sumOfRowErrors
  }

}
