import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spray.json._
import org.scalatest.Matchers
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.verify

@RunWith(classOf[JUnitRunner])
class NumericalDifferentiatorTest extends FunSuite with Matchers with MockitoSugar  {

  test("accepts 'backward' as differentiator strategy") {
    NumericalDifferentiator("backward", 0, 1)
  }

  test("accepts 'central' as differentiator strategy") {
    NumericalDifferentiator("central", 0, 1)
  }

  test("indexes of variables cannot be the same") {
    intercept[IllegalArgumentException] {
      NumericalDifferentiator("central", 1, 1)
    }
  }


}
