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
    val json =
      """
         {
            "function": "123.456"
         }
      """.parseJson

    val parameters = List[Double]()

    // When
    val function = Function.buildFunction(json.asJsObject)

    // Then
    val evaluatedValue = function.evaluate(parameters)
    evaluatedValue shouldBe (123.456 +- 0.0001)
  }

  test("create sin function") {
    // Given
    val json =
      """
         {
            "function": "sin",
            "operands": [{
                "function": "1.0"
            }]
         }
      """.parseJson

    val parameters = List[Double]()

    // When
    val function = Function.buildFunction(json.asJsObject)

    // Then
    function.evaluate(parameters) shouldBe (math.sin(1.0))
  }

  test("create plus function") {
    // Given
    val json =
      """
         {
            "function": "plus",
            "operands": [{
                "function": "1.0"
            }, {
                "function": "2.0"
            }]
         }
      """.parseJson

    val parameters = List[Double]()

    // When
    val function = Function.buildFunction(json.asJsObject)

    // Then
    function.evaluate(parameters) shouldBe 3.0
  }

}
