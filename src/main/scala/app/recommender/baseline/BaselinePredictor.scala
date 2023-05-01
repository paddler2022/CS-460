package app.recommender.baseline

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state: RDD[(Int, Int, Double, Double, Double)] = null
  private var movie_avg_ratings: RDD[(Int, Double)] = null
  private var user_avg_ratings: RDD[(Int, Double)] = null
  private var global_average: Double = 0.0
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    user_avg_ratings = ratingsRDD.map{case(user_id, _, _, rating, _) =>
      (user_id, (rating, 1))
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x=> x._1/x._2)

    val temp = ratingsRDD.map{case(user_id, movie_id, _, rating, _) => (user_id, (movie_id, rating))}
      .join(user_avg_ratings).map{case(user_id, ((movie_id,rating), user_avg)) =>
        val user_avg_dev = (rating - user_avg) /Scale(rating, user_avg)
        (user_id, movie_id, rating, user_avg, user_avg_dev)
      }

    movie_avg_ratings = temp.map { case (_, movie_id, _, _, user_avg_dev) =>
      (movie_id, (user_avg_dev, 1))
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)

    state = temp

    global_average = state.map { case (user_id, movie_id, rating, _, _) =>
      ((user_id, movie_id), (rating, 1))
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2).first()._2
  }

  def Scale(x: Double, y: Double): Double = {
    var result: Double = 0.0
    if (x > y) {
      result = 5 - y
    }
    else if (x < y) {
      result = y - 1
    }
    else {
      result = 1.0
    }
    result
  }

  def predict(userId: Int, movieId: Int): Double = {
    var p: Double = 0.0
    val user_avg = user_avg_ratings.filter { case (id, _) => id == userId }.map(_._2).take(1).headOption match {
      case Some(avg) => avg
      case None => global_average
    }
    val movie_avg = movie_avg_ratings.filter { case (id, _) => id == movieId }.map(_._2).take(1).headOption match {
      case Some(avg) => avg
      case None => 0.0
    }

    if (movie_avg == 0.0 && user_avg != global_average) {
      p = user_avg
    }
    else if (user_avg == global_average) {
      p = global_average
    }
    else {
      p = user_avg + movie_avg * Scale((user_avg + movie_avg), user_avg)
    }
    p
  }
}
