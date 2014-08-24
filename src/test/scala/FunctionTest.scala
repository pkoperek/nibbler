import org.junit.runner.RunWith
import org.mockito.Mockito.verify
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import spray.json._

@RunWith(classOf[JUnitRunner])
class FunctionTest extends FunSuite with MockitoSugar with ShouldMatchers {

  test("evaluate should invoke inner tree evaluate") {
    // Given
    val dataRow = List[Double]()
    val functionTree = mock[FunctionNode]

    // When
    new Function(functionTree).evaluate(dataRow)

    // Then
    verify(functionTree).evaluate(dataRow)
  }

  test("create function evaluating variable") {
    // Given
    val json = """
         {
            "function": "var_0"
         }
               """.parseJson

    // When
    val function = Function.buildFunction(json.asJsObject)
    val evaluatedValue = function.evaluate(List(3.1415))

    // Then
    evaluatedValue should be(3.1415)
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
    evaluatedValue should be(123.456 plusOrMinus 0.0001)
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
    evaluatedValue should be(math.sin(1.0) plusOrMinus 0.001)
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
    evaluatedValue should be(math.cos(2.0) plusOrMinus 0.001)
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
    evaluatedValue should be(math.tan(2.5) plusOrMinus 0.001)
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
    evaluatedValue should be(math.exp(2.5) plusOrMinus 0.001)
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
    evaluatedValue should be === 3.0
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
    evaluatedValue should be === 6.0
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
    evaluatedValue should be === 3.0
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
    evaluatedValue should be === 35.0
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
    evaluatedValue should be(math.sin(math.cos(100.0)) plusOrMinus 0.0001)
  }

  test("FunctionNode stores name") {
    // Given
    val functionName = "sin"

    // When
    val storedFunctionName = new FunctionNode(functionName, List()).name()

    // Then
    storedFunctionName should equal(functionName)
  }

  test("differentiaties symbolically sin") {
    // Given
    val function = new Function(new FunctionNode("sin", List(new FunctionNode("x", List()))))

    // When
    val differentiated = function.differentiate("x")

    // Then
    differentiated.evaluate(List(1.0)) should equal(Math.sin(1.0) plusOrMinus 0.0001)
  }

  test("differentiaties by non existing variable returns the same function") {
    // Given
    val function = new Function(new FunctionNode("sin", List(new FunctionNode("x", List()))))

    // When
    val differentiated = function.differentiate("x")

    // Then
    differentiated.evaluate(List(1.0)) should equal(function.evaluate(List(1.0)) plusOrMinus 0.0001)
  }

  private def evaluateJsonWithParams(jsonText: String): Double = {
    val json = jsonText.parseJson
    val function = Function.buildFunction(json.asJsObject)
    function.evaluate(List())
  }
}
