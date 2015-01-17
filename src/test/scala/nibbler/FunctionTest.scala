package nibbler

import nibbler.evaluation
import nibbler.evaluation.{FunctionBuilder, FunctionNode}
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
    new evaluation.Function(functionTree).evaluate(dataRow)

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
    val function = FunctionBuilder.buildFunction(json.asJsObject)
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
    val storedFunctionName = node(functionName).name()

    // Then
    storedFunctionName should equal(functionName)
  }

  test("FunctionNode stores children") {
    // Given
    val childFunction = new FunctionNode("cos", List())
    val function = new FunctionNode("sin", List(childFunction))

    // When
    val children: Seq[FunctionNode] = function.children()

    // Then
    children should contain(childFunction)
  }

  test("differentiates symbolically a constant integer") {
    // Given
    val functionTree = constant("123")

    // When
    val differentiated = new evaluation.Function(functionTree).differentiate("var_0")

    // Then
    differentiated.evaluate(List(1.0)) should be(0.0)

  }

  test("differentiates symbolically a constant double") {
    // Given
    val functionTree = constant("123.0")

    // When
    val differentiated = function(functionTree).differentiate("var_0")

    // Then
    differentiated.evaluate(List(1.0)) should be(0.0)

  }

  test("differentiates symbolically a constant with a dot") {
    // Given
    val functionTree = constant("333.")

    // When
    val differentiated = function(functionTree).differentiate("var_0")

    // Then
    differentiated.evaluate(List(1.0)) should be(0.0)

  }

  test("differentiates symbolically sin") {
    // When
    val differentiated = function(sin(var_0)).differentiate("var_0")

    // Then
    differentiated.evaluate(List(1.0)) should be(Math.cos(1.0) plusOrMinus 0.0001)
  }

  test("differentiates symbolically cos") {
    // When
    val differentiated = function(cos(var_0)).differentiate("var_0")

    // Then
    differentiated.evaluate(List(1.0)) should be(-Math.sin(1.0) plusOrMinus 0.0001)
  }

  test("differentiates symbolically plus") {
    // Given
    val toDifferentiate: evaluation.Function = function(plus(sin(var_0), constant("10")))

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List(2.0)) should be(Math.cos(2.0) plusOrMinus 0.0001)
  }

  test("differentiates symbolically minus") {
    // Given
    val toDifferentiate: evaluation.Function = function(minus(constant("10"), sin(var_0)))

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List(3.0)) should be(-Math.cos(3.0) plusOrMinus 0.0001)
  }

  test("differentiates by non existing variable should return 0") {
    // When
    val differentiated = function(sin(var_0)).differentiate("var_999")

    // Then
    differentiated.evaluate(List(1.0)) should equal(0.0)
  }

  test("differentiates symbolically mul of two constants") {
    // Given
    val toDifferentiate = function(mul(constant("2.0"), constant("3.0")))

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List()) should equal(0.0)
  }

  test("differentiates symbolically mul of constant and a variable") {
    // Given
    val toDifferentiate = function(mul(constant("2.0"), variable("var_0")))

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List(123.0)) should equal(2.0)
  }

  test("differentiates symbolically mul a product of three") {
    // Given
    val toDifferentiate = function(mul(constant("2.0"), variable("var_0"), variable("var_1")))

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List(1000.0, 4.0)) should equal(8.0)
  }

  test("differentiates exp(var_0)") {
    // Given
    val toDifferentiate = function(exp(var_0))

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List(2.0)) should be(Math.exp(2.0) plusOrMinus 0.00001)
  }

  test("differentiates exp(2*var_0)") {
    // Given
    val toDifferentiate = function(exp(mul(constant("2.0"), var_0)))

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List(3.0)) should be(2.0 * Math.exp(2.0 * 3.0) plusOrMinus 0.00001)
  }

  test("differentiates 1/var_0") {
    // Given
    val toDifferentiate = function(div(constant("1.0"), var_0))

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List(5.0)) should be(-1 / (5.0 * 5.0) plusOrMinus 0.00001)
  }

  test("differentiates 1-2*var_0/var_0") {
    // Given
    val toDifferentiate = function(
      div(
        minus(
          constant("1.0"),
          mul(
            constant("2.0"),
            var_0)
        ),
        var_0
      )
    )

    // When
    val differentiated = toDifferentiate.differentiate("var_0")

    // Then
    differentiated.evaluate(List(5.0)) should be(-1 / (5.0 * 5.0) plusOrMinus 0.00001)
  }

  private def mul(operands: FunctionNode*) = {
    node("mul", operands.toList)
  }

  private def minus(operandLeft: FunctionNode, operandRight: FunctionNode) = {
    node("minus", List(operandLeft, operandRight))
  }

  private def plus(operandLeft: FunctionNode, operandRight: FunctionNode) = {
    node("plus", List(operandLeft, operandRight))
  }

  private def div(operandLeft: FunctionNode, operandRight: FunctionNode) = {
    node("div", List(operandLeft, operandRight))
  }

  private def exp(operand: FunctionNode) = {
    node("exp", operand)
  }

  private def sin(operand: FunctionNode) = {
    node("sin", operand)
  }

  private def cos(operand: FunctionNode) = {
    node("cos", operand)
  }

  private def var_0 = {
    node("var_0")
  }

  private def evaluateJsonWithParams(jsonText: String): Double = {
    val json = jsonText.parseJson
    val function = FunctionBuilder.buildFunction(json.asJsObject)
    function.evaluate(List())
  }

  private def variable(name: String) = {
    new FunctionNode(name, List())
  }

  private def function(tree: FunctionNode) = {
    new evaluation.Function(tree)
  }

  private def node(name: String, child: FunctionNode) = {
    new FunctionNode(name, List(child))
  }

  private def node(name: String, children: Seq[FunctionNode]) = {
    new FunctionNode(name, children)
  }

  private def node(name: String) = {
    new FunctionNode(name, List())
  }

  private def constant(constantValue: String): FunctionNode = {
    node(constantValue)
  }
}
