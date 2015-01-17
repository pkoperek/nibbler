package nibbler

import nibbler.evaluation.PairGenerator
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class PairGeneratorTest extends FunSuite with ShouldMatchers {

  val generator: PairGenerator = new PairGenerator()

  test("should generate empty sequence for 0") {
    val pairs = generator.generatePairs(0)

    pairs should be('empty)
  }

  test("should generate empty sequence for 1") {
    val pairs = generator.generatePairs(1)

    pairs should be('empty)
  }

  test("should generate single pair for 2") {
    val pairs = generator.generatePairs(2)

    pairs should contain ((0, 1))
    pairs should have length (1)
  }

  test("should generate 3 pairs for 3") {
    val pairs = generator.generatePairs(3)

    pairs should (contain ((0, 1)) and contain ((0, 2)) and contain ((1, 2)))
    pairs should have length (3)
  }
}
