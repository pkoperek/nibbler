package nibbler.evaluation

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

trait NumericalDifferentiator extends Serializable {
  def partialDerivative(input: RDD[Seq[Double]]): RDD[Double] = {
    if (!validateInput(input)) {
      throw new IllegalArgumentException("Dataset doesn't contain at least two values in sequence!")
    }

    partialDerivativeInternal(input)
  }

  def partialDerivativeInternal(input: RDD[Seq[Double]]): RDD[Double]

  def validateInput(input: RDD[Seq[Double]]): Boolean = {
    val firstSequence = input.first()
    firstSequence.size >= 2
  }
}

object NumericalDifferentiator extends Serializable {
  def apply(name: String, differentialQuotientDividend: Int, differentialQuotientDivisor: Int): NumericalDifferentiator = {

    if (differentialQuotientDividend == differentialQuotientDivisor) {
      throw new IllegalArgumentException("dividend index can't be equal to divisor index!")
    }

    name match {
      case "backward" => new BackwardNumericalDifferentiator(differentialQuotientDividend, differentialQuotientDivisor)
      case "central" => new CentralNumericalDifferentiator(differentialQuotientDividend, differentialQuotientDivisor)
    }
  }

  private def reverse(input: (Seq[Double], Long)): (Long, Seq[Double]) = {
    input.swap
  }

  /**
   * x_n - x_n-1
   */
  private class BackwardNumericalDifferentiator(differentialQuotientDividend: Int, differentialQuotientDivisor: Int) extends NumericalDifferentiator {
    override def partialDerivativeInternal(input: RDD[Seq[Double]]): RDD[Double] = {
      val minuend: RDD[(Long, Seq[Double])] = input.zipWithIndex().map(reverse)
      val subtrahend: RDD[(Long, Seq[Double])] = minuend.map(minus_1)

      minuend.join(subtrahend).map(differentiate)
    }

    private def differentiate(row: (Long, (Seq[Double], Seq[Double]))) = {
      val minuendRow = row._2._1
      val subtrahendRow = row._2._2

      (minuendRow(differentialQuotientDividend) - subtrahendRow(differentialQuotientDividend)) / (minuendRow(differentialQuotientDivisor) - subtrahendRow(differentialQuotientDivisor))
    }

    private def minus_1(row: (Long, Seq[Double])) = {
      (row._1 - 1, row._2)
    }
  }

  /**
   * x_n - x_n-2
   */
  private class CentralNumericalDifferentiator(differentialQuotientDividend: Int, differentialQuotientDivisor: Int) extends NumericalDifferentiator {
    override def partialDerivativeInternal(input: RDD[Seq[Double]]): RDD[Double] = {
      val minuend: RDD[(Long, Seq[Double])] = input.zipWithIndex().map(reverse)
      val subtrahend: RDD[(Long, Seq[Double])] = minuend.map(minus_2)

      minuend.join(subtrahend).map(differentiate)
    }

    private def differentiate(row: (Long, (Seq[Double], Seq[Double]))) = {
      val minuendRow = row._2._1
      val subtrahendRow = row._2._2

      (minuendRow(differentialQuotientDividend) - subtrahendRow(differentialQuotientDividend)) / (minuendRow(differentialQuotientDivisor) - subtrahendRow(differentialQuotientDivisor))
    }

    private def minus_2(row: (Long, Seq[Double])) = {
      (row._1 - 2, row._2)
    }
  }

}

