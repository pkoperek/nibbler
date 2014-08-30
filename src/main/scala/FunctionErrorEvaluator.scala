import org.apache.spark.rdd.RDD

class FunctionErrorEvaluator(differentiatorType: String) extends Serializable {

  private val pairGenerator = new PairGenerator

  def evaluate(input: RDD[Seq[Double]], functionDeserialized: Function) = {
    val variablePairs = pairGenerator.generatePairs(2)
    val results = new StringBuilder

    for (pair <- variablePairs) {
      val df_dx = functionDeserialized.differentiate("var_" + pair._1)
      val df_dy = functionDeserialized.differentiate("var_" + pair._2)

      val df_dx_evaluated = input.map(df_dx.evaluate).zipWithIndex().map(reverse)
      val df_dy_evaluated = input.map(df_dy.evaluate).zipWithIndex().map(reverse)

      val differentiator = NumericalDifferentiator(differentiatorType, pair._1, pair._2)

      val numericallyDifferentiated = differentiator.partialDerivative(input).zipWithIndex().map(reverse)

      val result: String = "Results: " +
        " dfdx cnt: " + df_dx_evaluated.count() +
        " dfdy cnt: " + df_dy_evaluated.count() +
        " numdiff cnt: " + numericallyDifferentiated.count()

      println("Partial result: " + result)

      results.append(result)
    }

    results.toString()

  }

  private def reverse(toReverse: (Double, Long)) = toReverse.swap

}
