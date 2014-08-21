import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class PairGeneratorTest extends FunSuite with ShouldMatchers {

  val generator: PairGenerator = new PairGenerator()

  test("should generate empty sequence for 0") {
    val pairs = generator.generatePairs(0)

    pairs should be('empty)
  }

  test("should generate single pair for 1") {
    val pairs = generator.generatePairs(1)

    pairs should equal((0, 1))
  }

}
