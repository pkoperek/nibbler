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
      val numericallyDifferentiated = differentiator.partialDerivative(rawData).zipWithIndex().map(reverseAndIncrementIdx)
      numericalDerivatives += (derivative -> numericallyDifferentiated)
    }

    numericalDerivatives.get(derivative).get
  }

  private def reverseAndIncrementIdx(row: (Double, Long)): (Long, Double) = {
    (row._2, row._1 + 1L)
  }

  def getNumberOfRows = numberOfRows

  def getNumberOfColumns = numberOfColumns

  def getRawData = rawData
}

