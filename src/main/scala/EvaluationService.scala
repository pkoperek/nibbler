import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * User: koperek
 * Date: 06.07.14
 * Time: 12:08
 */
class EvaluationService(timeSeries: RDD[(Long, Double)]) {

  def evaluate(toEvaluate: Double => Double): Double = {
    var error = 0.0


    timeSeries.count()

    //    val result: Double = Math.abs(deltaX / deltaY) - Math.abs(dfdy_val / dfdx_val)
    //    error += Math.log(1 + Math.abs(result))
    //
    //    return error
  }
}