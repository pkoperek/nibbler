package nibbler

import org.apache.spark.rdd.RDD
import spray.json._

import scala.collection.mutable

class DataSet(numberOfRows: Long, numberOfColumns: Long, rawData: RDD[Seq[Double]]) extends Serializable {

  private val numericalDerivatives = mutable.HashMap.empty[String, RDD[(Long, Double)]]

  def getNumericallyDifferentiated(differentiatorType: String, pair: (Int, Int)): RDD[(Long, Double)] = {
    val derivative = differentiatorType + pair
    if (!(numericalDerivatives contains derivative)) {
      val differentiator = NumericalDifferentiator(differentiatorType, pair._1, pair._2)
      val numericallyDifferentiated = differentiator.partialDerivative(rawData).zipWithIndex().map(reverse).map(incrementIdx)
      numericalDerivatives += (derivative -> numericallyDifferentiated)
    }

    numericalDerivatives.get(derivative).get
  }

  private def incrementIdx(row: (Long, Double)): (Long, Double) = {
    (row._1 + 1, row._2)
  }

  private def reverse(toReverse: (Double, Long)) = toReverse.swap

  def getNumberOfRows = numberOfRows

  def getNumberOfColumns = numberOfColumns

  def getRawData = rawData
}

