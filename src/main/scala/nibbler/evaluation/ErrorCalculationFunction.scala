package nibbler.evaluation

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class ErrorCalculationFunction extends Serializable {

  private def computeError(row: (Long, (Double, Double))) = {
    (1L, Math.log(Math.abs(row._2._1 - row._2._2) + 1.0))
  }

  private def add(left: (Long, Double), right: (Long, Double)) = {
    (left._1 + right._1, left._2 + right._2)
  }

  def calculateError(symbolicallyDifferentiated: RDD[(Long, Double)], numericallyDifferentiated: RDD[(Long, Double)]): Double = {
    val (count, sumOfRowErrors) = numericallyDifferentiated.join(symbolicallyDifferentiated)
      .map(computeError)
      .reduce(add)

    -sumOfRowErrors / count
  }

}
