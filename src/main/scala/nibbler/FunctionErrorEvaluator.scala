package nibbler

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class FunctionErrorEvaluator(differentiatorType: String) extends Serializable {

  private val errorCalculationFunction = new ErrorCalculationFunction
  private val pairGenerator = new PairGenerator

  def evaluate(input: RDD[Seq[Double]], functionDeserialized: Function): Double = {
    val variablePairs = pairGenerator.generatePairs(2)
    var error = Double.MinValue

    for (pair <- variablePairs) {
      val symbolicallyDifferentiated = symbolicDifferentiation(input, functionDeserialized, pair)
      val numericallyDifferentiated = numericalDifferentiation(input, pair)

      val pairingError = errorCalculationFunction.calculateError(symbolicallyDifferentiated, numericallyDifferentiated)

      if (error < pairingError) {
        error = pairingError
      }
    }

    error
  }

  private def symbolicDifferentiation(input: RDD[Seq[Double]], function: Function, pair: (Int, Int)): RDD[(Long, Double)] = {
    val df_dx = function.differentiate("var_" + pair._1)
    val df_dy = function.differentiate("var_" + pair._2)

    input.map(x => df_dy.evaluate(x) / df_dx.evaluate(x)).zipWithIndex().map(reverse)
  }

  private def divide(row: (Long, (Double, Double))) = {
    (row._1, row._2._1 / row._2._2)
  }

  private def numericalDifferentiation(input: RDD[Seq[Double]], pair: (Int, Int)): RDD[(Long, Double)] = {
    val differentiator = NumericalDifferentiator(differentiatorType, pair._1, pair._2)
    val numericallyDifferentiated = differentiator.partialDerivative(input).zipWithIndex().map(reverse).map(incrementIdx)
    numericallyDifferentiated
  }

  private def incrementIdx(row: (Long, Double)): (Long, Double) = {
    (row._1 + 1, row._2)
  }

  private def reverse(toReverse: (Double, Long)) = toReverse.swap

}
