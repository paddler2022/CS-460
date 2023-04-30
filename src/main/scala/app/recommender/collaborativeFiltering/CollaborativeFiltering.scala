

package app.recommender.collaborativeFiltering


import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}


class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val ratings = ratingsRDD.map { case (user_id, movie_id,_, rating, _) =>
      Rating(user_id, movie_id, rating)
    }
    // Build the ALS model
    val als = new ALS().setSeed(seed).setRank(rank).setIterations(maxIterations).setLambda(regularizationParameter)
      .setBlocks(n_parallel)
    model = als.run(ratings)
  }

  def predict(userId: Int, movieId: Int): Double = {
    val predict_rating = model.predict(userId, movieId)
    predict_rating
  }

}
