package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {

    val temp = sc.textFile(getClass.getResource(path).getPath)
    //val temp = sc.textFile(path)
    val rdd_MoviesLoader = temp.map { line =>
      val splitting = line.split("\\|")
      val movie_id = splitting(0).toInt
      val movie_name = splitting(1).replaceAll("\"", "") //eliminate all the " in the name
      //val movie_keywords = splitting(2).split("\\|").toList
      val seq = for (i <- 2 until splitting.length) yield splitting(i).replaceAll("\"", "")
      val movie_keywords = seq.toList
      (movie_id, movie_name, movie_keywords)
    }
    rdd_MoviesLoader
  }
}

