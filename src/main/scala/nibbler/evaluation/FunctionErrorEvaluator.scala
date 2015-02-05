package nibbler.evaluation

import nibbler.evaluation
import org.apache.spark.rdd.RDD

class FunctionErrorEvaluator() extends Serializable {

  private val errorCalculationFunction = new ErrorCalculationFunction
  private val pairGenerator = new PairGenerator

  def evaluate(input: DataSet, functionDeserialized: evaluation.Function): Double = {
    val variablePairs = pairGenerator.generatePairs(2)
    var error = Double.MinValue

    for (pair <- variablePairs) {
      val data = input.getRawWithNumericallyDifferentiated(pair)
      val symbolicallyDifferentiated = symbolicDifferentiation(data, functionDeserialized, pair)

      val errorAggregated = errorCalculationFunction.calculateError(symbolicallyDifferentiated)
      val pairingError = errorAggregated / input.getNumberOfRows

      if (error < pairingError) {
        error = pairingError
      }
    }

    error
  }

  private def symbolicDifferentiation(input: RDD[(Seq[Double], Double)], function: evaluation.Function, pair: (Int, Int)): RDD[(Double, Double)] = {
    val df_dx = function.differentiate("var_" + pair._1)
    val df_dy = function.differentiate("var_" + pair._2)

    input.map(x =>
      (df_dy.evaluate(x._1) / df_dx.evaluate(x._1), x._2)
    )
  }

}
