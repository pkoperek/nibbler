package nibbler

import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.matchers.ShouldMatchers

class SavingTextFileTest extends FunSuite with MockitoSugar with ShouldMatchers with SparkContextAware {

  test("create a file in context") {
    // given
    val filename = "/tmp/test-stored.txt"
    val rddToStore = sparkContext.parallelize(List(
      List(10L, 0.1),
      List(11L, 0.2)
    )).map(l => l(0) + "," + l(1))

    // when
    rddToStore.saveAsTextFile(filename)
    val reread = sparkContext.textFile(filename).map(s => {
      val splitted = s.split(",")
      (splitted(0).toLong, splitted(0).toDouble)
    })

    // then
    println(reread.collect())
  }



}
