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
    try { 
      NumericalDifferentiator("backward", 0, 1)
    } catch {
      case _: Exception => fail("shouldn't throw an exception!")
    }
  }

  test("accepts 'central' as differentiator strategy") {
    try { 
      NumericalDifferentiator("central", 0, 1)
    } catch {
      case _: Exception => fail("shouldn't throw an exception!")
    }
  }

  test("indexes of variables cannot be the same") {
    intercept[IllegalArgumentException] {
      NumericalDifferentiator("central", 1, 1)
    }
  }


}
