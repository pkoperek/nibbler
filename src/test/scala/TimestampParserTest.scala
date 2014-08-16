import org.scalatest.{Matchers, FunSuite}
import org.scalatest.mock.MockitoSugar

class TimestampParserTest extends FunSuite with MockitoSugar with Matchers {

  test("should parse timestamp") {

    // Given
    val timestampAsString = "20000530 172736000"

    // When
    val parser = new TimestampParser
    val timestamp = parser.parse(timestampAsString)

    // Then
    timestamp shouldBe 959704056000L
  }

}
