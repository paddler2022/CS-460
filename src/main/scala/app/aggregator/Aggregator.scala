package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.{MEMORY_AND_DISK, MEMORY_ONLY}

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = null
  private var movie_name_ratings: RDD[(Int, (String, Double, Int, List[String]))] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *                format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {

    val movies_id_name_ratings = ratings
      .map { case (_, title_id, _, rating, _) => (title_id, (rating, 1)) }
      .aggregateByKey((0.0, 0))(
        (acc, value) => (acc._1 + value._1, acc._2 + value._2),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      .mapValues { case (ratings_sum, counts) => (ratings_sum / counts.toDouble, counts)}
      .fullOuterJoin(title.groupBy(_._1))
      .flatMapValues { case (value1, values2) => values2.map(value2 =>
        (value1.map(_._1).getOrElse(0.0), value1.map(_._2).getOrElse(0),value2)) }
      .map { case (movie_id, (avg_rating, counts, value2)) => (movie_id, avg_rating, counts, value2) }

    val movie_name_ratings_temp = movies_id_name_ratings.flatMap { case (movie_id, rating, counts, iter) =>
      iter.map { case (int_1, movie_name, keywords) =>
        (movie_id, rating, counts, (int_1, movie_name, keywords))
      }
    }.map { case (movie_id, rating, counts, movie_info) =>
      (movie_id, movie_info._2, rating, counts, movie_info._3)
    }

    partitioner = new HashPartitioner(ratings.getNumPartitions)
    movie_name_ratings = movie_name_ratings_temp.map{case(id, name, rating, counts, genre)=>
      (id, (name, rating, counts, genre))}.partitionBy(partitioner).persist(MEMORY_ONLY)

  }
  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {

    movie_name_ratings.map { case (id, (name, rating, counts, genre)) =>
      (id, name, rating, counts, genre)
    }.map{case(_, movie_name, movie_rating, _, _) => (movie_name, movie_rating)}
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    // Filter movies that contain all keywords
    val relevant_movies = movie_name_ratings.map { case (id, (name, rating, counts, genre))=>
      (id, name, rating, counts, genre)
    }.filter { case (_, _, _, _, keywords_list) =>
      keywords.forall(keywords_list.contains)
    }
    // Compute average rating for relevant movies
    val (total_rating, num_movies) = relevant_movies.aggregate((0.0, 0))(
      (acc, movie_info) => (acc._1 + movie_info._3, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    if (num_movies == 0) {
      -1.0 // No such titles exist
    } else if (total_rating == 0.0) {
      0.0 // Such titles are unrated
    } else {
      total_rating / num_movies.toDouble // For those with full info, compute average rating
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */


  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val delta_by_movie_id = sc.parallelize(delta_).map { case (_, title_id, old_rating, rating, _) =>
      (title_id, (old_rating, rating))
    }.aggregateByKey((0.0, 0))((acc, ratings) => {
      val (oldRating, newRating) = ratings
      val oldRatingValue = oldRating.getOrElse(0.0)
      if (oldRatingValue == 0.0){
        (acc._1 + newRating, acc._2 + 1)
      }
      else{
        (acc._1 - oldRatingValue + newRating, acc._2)
      }
    }, (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val temp = movie_name_ratings
    movie_name_ratings.unpersist()
    val join_rdd = temp.leftOuterJoin(delta_by_movie_id)
    val updated_rdd = join_rdd.mapValues {
      case ((movie_name, avg_rating, counts, movie_genre), Some((sum, cnt))) =>
        val new_counts = counts + cnt
        val new_avg = (avg_rating*counts + sum)/new_counts
        (movie_name, new_avg, new_counts, movie_genre)
      case ((movie_name, avg_rating, counts, movie_genre),None) =>
        (movie_name, avg_rating, counts, movie_genre)
    }
    movie_name_ratings = updated_rdd.partitionBy(partitioner).persist(MEMORY_ONLY)
  }
}
