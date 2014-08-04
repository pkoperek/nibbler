import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spray.json._
import org.scalatest.Matchers
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.verify

/**
 *
 * User: koperek
 * Date: 03.08.14
 * Time: 18:24
 */
@RunWith(classOf[JUnitRunner])
class FunctionTest extends FunSuite with MockitoSugar with Matchers {

  test("evaluate should invoke inner tree evaluate") {
    // Given
    val dataRow = List[Double]()
    val functionTree = mock[FunctionNode]

    // When
    new Function(functionTree).evaluate(dataRow)

    // Then
    verify(functionTree).evaluate(dataRow)
  }

  test("create constant function") {
    // Given
    val jsonText = """
         {
            "function": "123.456"
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe (123.456 +- 0.0001)
  }

  test("create sin function") {
    // Given
    val jsonText = """
         {
            "function": "sin",
            "operands": [{
                "function": "1.0"
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe (math.sin(1.0) +- 0.001)
  }

  test("create cos function") {
    // Given
    val jsonText = """
         {
            "function": "cos",
            "operands": [{
                "function": "2.0"
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe (math.cos(2.0) +- 0.001)
  }

  test("create tg function") {
    // Given
    val jsonText = """
         {
            "function": "tan",
            "operands": [{
                "function": "2.5"
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe (math.tan(2.5) +- 0.001)
  }

  test("create exp function") {
    // Given
    val jsonText = """
         {
            "function": "exp",
            "operands": [{
                "function": "2.5"
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe (math.exp(2.5) +- 0.001)
  }

  test("create plus function") {
    //Given
    val jsonText = """
         {
            "function": "plus",
            "operands": [{
                "function": "1.0"
            }, {
                "function": "2.0"
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe 3.0
  }

  test("create minus function") {
    //Given
    val jsonText = """
         {
            "function": "minus",
            "operands": [{
                "function": "8.0"
            }, {
                "function": "2.0"
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe 6.0
  }

  test("create mul function") {
    //Given
    val jsonText = """
         {
            "function": "mul",
            "operands": [{
                "function": "1.5"
            }, {
                "function": "2.0"
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe 3.0
  }

  test("create div function") {
    //Given
    val jsonText = """
         {
            "function": "div",
            "operands": [{
                "function": "70.0"
            }, {
                "function": "2.0"
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe 35.0
  }

  test("create nested function") {
    //Given
    val jsonText = """
         {
            "function": "sin",
            "operands": [{
                "function": "cos",
                "operands": [{
                    "function": "100.0"
                }]
            }]
         }
                   """

    // When
    val evaluatedValue = evaluateJsonWithParams(jsonText)

    // Then
    evaluatedValue shouldBe (math.sin(math.cos(100.0)) +- 0.0001)
  }

  private def evaluateJsonWithParams(jsonText: String): Double = {
    val json = jsonText.parseJson
    val function = Function.buildFunction(json.asJsObject)
    function.evaluate(List())
  }
}
