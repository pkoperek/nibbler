package nibbler

import org.apache.spark.rdd.RDD
import spray.json._

class DataSet(numberOfRows: Long, numberOfColumns: Long, rawData: RDD[String]) {
  def getNumberOfRows = numberOfRows

  def getNumberOfColumns = numberOfColumns

  def getRawData = rawData
}

