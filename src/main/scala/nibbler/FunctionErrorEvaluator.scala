package nibbler

import org.apache.spark.rdd.RDD

class FunctionErrorEvaluator(differentiatorType: String) extends Serializable {

  private val errorCalculationFunction = new ErrorCalculationFunction
  private val pairGenerator = new PairGenerator

  def evaluate(input: DataSet, functionDeserialized: Function): Double = {
    val variablePairs = pairGenerator.generatePairs(input.getNumberOfColumns)
    var error = Double.MinValue

    for (pair <- variablePairs) {
      val symbolicallyDifferentiated = symbolicDifferentiation(input, functionDeserialized, pair)
      val numericallyDifferentiated = input.getNumericallyDifferentiated(pair)

      val pairingError = errorCalculationFunction.calculateError(symbolicallyDifferentiated, numericallyDifferentiated)

      if (error < pairingError) {
        error = pairingError
      }
    }

    error
  }

  private def symbolicDifferentiation(input: DataSet, function: Function, pair: (Int, Int)): RDD[(Long, Double)] = {
    val df_dx = function.differentiate("var_" + pair._1)
    val df_dy = function.differentiate("var_" + pair._2)

    input.getRawData.map(x => df_dy.evaluate(x) / df_dx.evaluate(x)).zipWithIndex().map(reverse)
  }

  private def reverse(toReverse: (Double, Long)) = toReverse.swap

}
