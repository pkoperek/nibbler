package nibbler

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class ErrorCalculationFunction extends Serializable {

  private def minus(row: (Long, (Double, Double))) = {
    (row._1, row._2._1 - row._2._2)
  }

  private def setIndexTo_1(row: (Long, Double)) = {
    (1L, row._2)
  }

  private def abs(row: (Long, Double)) = {
    (row._1, Math.abs(row._2))
  }

  private def log(row: (Long, Double)) = {
    (row._1, Math.log(row._2))
  }

  private def add_1(row: (Long, Double)) = {
    (row._1, row._2 + 1.0)
  }

  private def add(left: (Long, Double), right: (Long, Double)) = {
    (left._1 + right._1, left._2 + right._2)
  }

  def calculateError(symbolicallyDifferentiated: RDD[(Long, Double)], numericallyDifferentiated: RDD[(Long, Double)]): Double = {
    val (count, sumOfRowErrors) = numericallyDifferentiated.join(symbolicallyDifferentiated)
      .map(minus)
      .map(setIndexTo_1)
      .map(abs)
      .map(add_1)
      .map(log)
      .reduce(add)

    -sumOfRowErrors / count
  }

}
