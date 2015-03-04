package nibbler.io

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.joda.time.format.DateTimeFormat

class HistdataTimestampParser extends Serializable {

  val dateFormat = DateTimeFormat.forPattern("yyyyMMdd HHmmssSSS")

  def parse(timestamp: String): Long = {
    dateFormat.parseMillis(timestamp)
  }

}
