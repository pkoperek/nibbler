package nibbler

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

class HistdataTimestampParser extends Serializable {

  val dateFormat = new SimpleDateFormat("yyyyMMdd HHmmssSSS", Locale.US)

  def parse(timestamp: String): Long = {
    val parsedTimestamp: Date = dateFormat.parse(timestamp)
    parsedTimestamp.getTime
  }

}
