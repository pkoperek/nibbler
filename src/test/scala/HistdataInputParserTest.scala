import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class HistdataInputParserTest extends FunSuite with MockitoSugar with ShouldMatchers with SparkContextAware {

  private val inputParser = new HistdataInputParser()

  test("reads multiple rows") {
    // Given
    val input = List("20000530 172736000,0.1","20000530 172737000,0.5")

    // When
    val parsed: Array[Seq[Double]] = parse(input)

    // Then
    parsed should have length 2
  }

  test("preserves order when reading multiple rows") {
    // Given
    val input = List("20000530 172736000,0.1","20000530 172737000,0.5")

    // When
    val parsed: Array[Seq[Double]] = parse(input)

    // Then
    parsed(0)(1) should equal(0.1)
    parsed(1)(1) should equal(0.5)
  }

  test("reads only a timestamp") {
    // Given
    val input = List("20000530 172736000,0.930200")

    // When
    val parsed: Array[Seq[Double]] = parse(input)

    // Then
    parsed(0)(0) should equal(959704056000.0)
  }

  test("reads multiples numbers after timestamp") {
    // Given
    val input = List("20000530 172736000,0.1,0.2,0.3")

    // When
    val parsed: Array[Seq[Double]] = parse(input)

    // Then
    parsed(0) should (contain(0.1) and contain(0.2) and contain(0.3))
  }

  test("first parsed element is timestamp") {
    // Given
    val input = List("20000530 172736000,0.930200")

    // When
    val parsed: Array[Seq[Double]] = parse(input)

    // Then
    parsed(0)(0) should equal(959704056000.0)
  }

  test("parses input with single line and single number") {

    // Given
    val input = List("20000530 172736000,0.930200")

    // When
    val parsed: Array[Seq[Double]] = parse(input)

    // Then
    parsed should have length 1
    parsed(0) should (contain(959704056000.0) and contain(0.9302))
  }

  test("read a single row from a line of input") {
    // Given
    val input = List("20000530 172736000,0.930200")

    // When
    val parsed: Array[Seq[Double]] = parse(input)

    // Then
    parsed should have length 1
  }

  private def parse(input: List[String]): Array[Seq[Double]] = {
    inputParser.parse(sparkContext.parallelize(input)).collect()
  }

}
