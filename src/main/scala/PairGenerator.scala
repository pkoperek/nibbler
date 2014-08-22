import scala.collection.mutable.ListBuffer

class PairGenerator {

  def generatePairs(numberOfElements: Int): Seq[(Int, Int)] = {
    if(numberOfElements >= 2) {
      val buffer = ListBuffer[(Int, Int)]()

      for(i <- 0 to numberOfElements-2) {
        for(j <- i+1 to numberOfElements-1) {
          buffer.append((i, j))
        }
      }

      buffer.toList
    } else {
      List()
    }
  }

}
