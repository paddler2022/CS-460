package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val user_movie_pairs = ratings.map{case(uid, mid, _, _ ,_) => (uid, mid)}.collect().toList
    val genre_rdd = sc.parallelize(List(genre))
    val get_movies_1 = nn_lookup.lookup(genre_rdd).flatMap { case (_, movie_list) => movie_list }
      .map { case (movie_id, _, _) => movie_id } //.collect().toList

    val get_movies_filtered = get_movies_1.filter { case (movieId) => !user_movie_pairs.contains((userId, movieId)) }.collect().toList

    val get_movie_2 = get_movies_filtered.map(x => (x, baselinePredictor.predict(userId, x)))
    val get_movies = get_movie_2.sortBy(_._2).reverse.take(K)

    get_movies

  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    val user_movie_pairs = ratings.map{case(uid, mid, _, _ ,_) => (uid, mid)}.collect().toList
    val genre_rdd = sc.parallelize(List(genre))
    val get_movies_1 = nn_lookup.lookup(genre_rdd).flatMap { case (_, movie_list) => movie_list }
      .map { case (movie_id, _, _) => movie_id }//.collect().toList

    val get_movies_filtered = get_movies_1.filter{case(movieId) => !user_movie_pairs.contains((userId, movieId))}.collect().toList

    val get_movie_2 = get_movies_filtered.map(x => (x, collaborativePredictor.predict(userId, x)))
    val get_movies = get_movie_2.sortBy(_._2).reverse.take(K)

    get_movies
  }
}
