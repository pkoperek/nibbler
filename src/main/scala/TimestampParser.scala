import java.text.SimpleDateFormat
import java.util.{Locale, Date}

class TimestampParser extends Serializable {

  val dateFormat = new SimpleDateFormat("yyyyMMdd HHmmssSSS", Locale.US)

  def parse(timestamp: String): Long = {
    val parsedTimestamp: Date = dateFormat.parse(timestamp)
    parsedTimestamp.getTime
  }

}
