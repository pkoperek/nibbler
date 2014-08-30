class HistdataInputParser extends Serializable {

  private val timestampParser = new TimestampParser

  def parseLine(row: String) = {
    val splitted = row.split(",")
    val timestamp: Long = timestampParser.parse(splitted(0))
    List(timestamp.toDouble) ++ splitted.splitAt(1)._2.map(_.toDouble).toList
  }

}
