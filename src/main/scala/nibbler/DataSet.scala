package nibbler

import org.apache.spark.rdd.RDD
import spray.json._

class DataSet(
               numberOfRows: Long,
               numberOfColumns: Long,
               rawData: RDD[Seq[Double]],
               numericallyDifferentiated: Map[(Int, Int), RDD[(Long, Double)]]
               ) {

  def getNumericallyDifferentiated(pair: (Int, Int)): RDD[(Long, Double)] = {
    numericallyDifferentiated(pair)
  }

  def getNumberOfRows = numberOfRows

  def getNumberOfColumns = numberOfColumns

  def getRawData = rawData
}

