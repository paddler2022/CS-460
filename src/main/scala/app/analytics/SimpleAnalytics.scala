package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.{MEMORY_AND_DISK, MEMORY_ONLY}

import java.time.{Instant, LocalDateTime, ZoneId}

class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  private var titlesGroupedById: RDD[(Int, Iterable[(Int, String, List[String])])] = null
  private var ratingsGroupedByYearByTitle: RDD[(Int, Map[Int, Iterable[(Int, Int, Option[Double], Double, Int)]])] = null


  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movies: RDD[(Int, String, List[String])]
          ): Unit = {
    val ratings_by_year = ratings.map{rating =>
      val dateTiming = Instant.ofEpochSecond(rating._5)
      val year = LocalDateTime.ofInstant(dateTiming, ZoneId.systemDefault()).getYear
      (rating._1, rating._2, rating._3, rating._4, year)
    }.groupBy(_._5)

    val ratings_by_year_by_movie_id = ratings_by_year.distinct().mapValues(ratings => ratings.groupBy(_._2))
    val movies_by_id = movies.groupBy(_._1)

    ratingsPartitioner = new HashPartitioner(1)//(ratings_by_year_by_movie_id.getNumPartitions)
    moviesPartitioner = new HashPartitioner(2)//(movies_by_id.getNumPartitions)

    ratingsGroupedByYearByTitle = ratings_by_year_by_movie_id.partitionBy(ratingsPartitioner).persist(MEMORY_ONLY)
    titlesGroupedById = movies_by_id.partitionBy(moviesPartitioner).persist(MEMORY_ONLY)

  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    val count_by_year = ratingsGroupedByYearByTitle.map{case(year, movieRatings) => (year, movieRatings.keys.size)}
    count_by_year
  }


  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    val topRatedMovies = ratingsGroupedByYearByTitle.mapValues (
      ratings =>
        ratings.mapValues(a=>a.size).toList.sortWith {
          case ((id_1, cnt_1), (id_2, cnt_2)) => {
            if (cnt_1 == cnt_2) id_1 > id_2
            else cnt_1 > cnt_2
          }
        }.head._1
    ).map{case(x, y)=>(y, x)}

    val result = topRatedMovies.join(titlesGroupedById.mapValues(movie_info => movie_info.head))
      .map({
        case (_, (year, (_, movie_name, _))) => (year, movie_name)})
    result
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    val topRatedMovies = ratingsGroupedByYearByTitle.mapValues(
      ratings =>
        ratings.mapValues(a => a.size).toList.sortWith {
          case ((id_1, cnt_1), (id_2, cnt_2)) => {
            if (cnt_1 == cnt_2) id_1 > id_2
            else cnt_1 > cnt_2
          }
        }.head._1
    ).map { case (x, y) => (y, x) }

    val result = topRatedMovies.join(titlesGroupedById.mapValues(movie_info => movie_info.head))
      .map({
        case (_, (year, (_, _, movie_genre))) => (year, movie_genre)
      })
    result
  }


  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {

    val genres_most_rated = getMostRatedGenreEachYear
    if (genres_most_rated.isEmpty()) {
      return (("No", 1), ("No", 2))
    }
    val genres_counts = genres_most_rated.flatMap(x => x._2.map(genre => (genre,1)))
    val genres_counts_sum = genres_counts.reduceByKey((x , y) => x + y)
    val most_popular_genre = genres_counts_sum.sortBy(x=>(-x._2, x._1)).first()
    val least_popular_genre = genres_counts_sum.sortBy(x => (x._2, x._1)).first()
    (least_popular_genre, most_popular_genre)
  }


  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    val All_Movies_By_Genre = movies.filter(movie => movie._3.intersect(requiredGenres.toString()).nonEmpty)
      .map(movie => movie._2)

    All_Movies_By_Genre
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    val broadcastGenres = broadcastCallback(requiredGenres)

    val All_Movies_By_Genre = movies.filter(movie => movie._3.intersect(broadcastGenres.value).nonEmpty)
      .map(movie => movie._2)

    All_Movies_By_Genre
  }

}

