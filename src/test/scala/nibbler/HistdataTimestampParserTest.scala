package nibbler

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar

class HistdataTimestampParserTest extends FunSuite with MockitoSugar with ShouldMatchers {

  test("should parse timestamp") {

    // Given
    val timestampAsString = "20000530 172736000"

    // When
    val parser = new HistdataTimestampParser
    val timestamp = parser.parse(timestampAsString)

    // Then
    timestamp should equal(959704056000L)
  }

}
