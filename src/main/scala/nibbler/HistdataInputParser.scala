package nibbler

object HistdataInputParser extends Serializable {

  private val timestampParser = new HistdataTimestampParser

  def parseLine(row: String): Seq[Double] = {
    val splitted = row.split(",")
    val timestamp: Long = timestampParser.parse(splitted(0))
    val numbers = splitted.splitAt(1)._2
    List(timestamp.toDouble) ++ numbers.map(_.toDouble).toList
  }

}
