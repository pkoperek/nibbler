package nibbler.evaluation

import org.apache.spark.rdd.RDD

class DataSet(numberOfRows: Long, numberOfColumns: Long, rawData: RDD[Seq[Double]]) {
  def getNumberOfRows = numberOfRows

  def getNumberOfColumns = numberOfColumns

  def getRawData = rawData
}

