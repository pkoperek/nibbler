package nibbler.evaluation

import org.apache.spark.rdd.RDD

class DataSet(numberOfRows: Long, numberOfColumns: Long, numDifferentiated: Map[(Int, Int), RDD[(Long, (Seq[Double], Double))]]) {
  def getNumberOfRows = numberOfRows

  def getNumberOfColumns = numberOfColumns

  def getRawWithNumericallyDifferentiated = numDifferentiated
}

