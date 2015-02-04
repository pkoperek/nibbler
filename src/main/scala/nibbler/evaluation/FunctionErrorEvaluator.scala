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

  private def symbolicDifferentiation(input: RDD[(Long, (Seq[Double], Double))], function: evaluation.Function, pair: (Int, Int)): RDD[(Long, (Double, Double))] = {
    val df_dx = function.differentiate("var_" + pair._1)
    val df_dy = function.differentiate("var_" + pair._2)

    input.map(x =>
      (x._1,
        (
          df_dy.evaluate(x._2._1) / df_dx.evaluate(x._2._1)
          ,
          x._2._2
          )
        )
    )
  }

}
