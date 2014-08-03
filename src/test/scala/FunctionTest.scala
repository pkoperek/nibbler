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

    // When
    val function = Function.buildFunction(json.asJsObject)

    // Then
    val evaluatedValue = function.evaluate(List[Double]())
    evaluatedValue shouldBe (123.456 +- 0.0001)
  }

}
