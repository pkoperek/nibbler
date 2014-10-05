package nibbler

import org.apache.spark.rdd.RDD
import spray.json._

class DataSet(
               size: (Long, Int),
               rawData: RDD[Seq[Double]],
               numericallyDifferentiated: Map[(Int, Int), RDD[(Long, Double)]]
               ) {

  def getNumericallyDifferentiated(pair: (Int, Int)): RDD[(Long, Double)] = {
    numericallyDifferentiated(pair)
  }

  def getNumberOfRows = size._1

  def getNumberOfColumns = size._2

  def getRawData = rawData
}

