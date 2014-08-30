import org.apache.spark.rdd.RDD

class HistdataInputParser extends Serializable {

  private val timestampParser = new HistdataTimestampParser

  def parse(textInput: RDD[String]): RDD[Seq[Double]] = {
    textInput.map(parseLine)
  }

  private def parseLine(row: String) = {
    val splitted = row.split(",")
    val timestamp: Long = timestampParser.parse(splitted(0))
    val numbers = splitted.splitAt(1)._2
    List(timestamp.toDouble) ++ numbers.map(_.toDouble).toList
  }

}
