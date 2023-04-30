package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, result) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val buckets = lshIndex.getBuckets()
    val hashed = lshIndex.hash(queries)
    val result = lshIndex.lookup(hashed).map{
      case(_, keyword_list, result_list)=>
        (keyword_list, result_list)
    }
    result
  }
}
