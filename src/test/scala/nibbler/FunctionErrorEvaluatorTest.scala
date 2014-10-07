package nibbler

import nibbler.FunctionBuilder.{node, plus, sin}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
@Ignore
class FunctionErrorEvaluatorTest extends FunSuite with MockitoSugar with ShouldMatchers with SparkContextAware {

  test("evaluates sin(x)") {
    // Given
    val function = f(sin(plus(variable(0), variable(1))))

    val input = List(
      List(10.0, 0.1),
      List(11.0, 0.2)
    )

    // When
    val error = evaluate(function, input)

    // Then
    error should be(-2.30258 plusOrMinus 0.00001)
  }

  private def evaluate(function: Function, input: Seq[Seq[Double]]): Double = {
    new FunctionErrorEvaluator().evaluate(
      DataSet((input.size, input(0).size), sparkContext.parallelize(input)),
      function
    )
  }

  private def variable(idx: Int): FunctionNode = {
    node("var_" + idx)
  }

  private def f(tree: FunctionNode): Function = {
    new Function(tree)
  }

}
