#Here is the README for CS460 project of Yuheng Lu

##DataLoader

First we need to load the data from csv files. I use the function "getClass.getResource(path).getPath" to  get the path of the csv files  so that the code can be run in different environments.

###class MoviesLoader

I used sc.textfile to read the data and then the data is in the format of lines with every attribute seperated by the "|". Then I split them and get every single attribute. Since attribute "movie_name" is read with '"', I need to eliminate '"'. Besides, the movie_keywords were firstly in the format of "A|B|C|...", I need to transform it into List.

###class RatingsLoader

Similar to the reading in movies, we load the ratings in lines. For the line in the format of "user_id|movie_id|rating|timestamp", we need to add a new attribute "old_rating" with value "None". For the line in the format of "user_id|movie_id|old_rating|new_rating|timestamp", we need to check whether theattribute "old_rating" has value. If so, we fill the real number;otherwise, use "None". 

##SimpleAnalytics

I used "private var titlesGroupedById" and "private var ratingsGroupedByYearByTitle" to store the preprocessed data in init. 

titlesGroupedById: RDD:[(movie_id, Iterable[(movie_id, movie_name, movie_keywords)])]

ratingsGroupedByYearByTitle: RDD[(movie_year, Map[movie_id, Iterable[(user_id, movie_id, old_rating, rating, movie_year)]])]

###init

Just do the certain group and partition operations, and store the preprocessed data.

###getNumberOfMoviesRatedEachYear

Since the ratingsGroupedByYearByTitle is first grouped by year, we can just count the number of the values(size of values) for each year, and return the RDD:[movie_year, counts].

###getMostRatedMovieEachYear

We first use mapValue to get the pair of (movie_year, counts) and then first sort by the number of counts and then do join operations. Finally, according to the requirement, I only reserve (movie_year, movie_name) in RDD.

###getMostRatedGenreEachYear

Similar to the above function, this time we only reserve (movie_year, movie_genre) in RDD.

###getMostAndLeastRatedGenreAllTime

We do the getMostRatedGenreEachYear and get the RDD:[(movie_year, movie_genre)]. We then counts the total number of genres and sort by the counts both ascending and descending. Finally, return the first tuple respectively.

###getAllMoviesByGenre

Use intersect function to get the intersection of the existing genres and required genres.

###getAllMoviesByGenre_usingBroadcast

The design is similar, but use broadcast method instead.


##Aggregator

###init

I use the variate "private var movie_name_ratings" to store the preprocessed data. 

movie_name_ratings: RDD:[(moive_id, (movie_name, movie_average_rating, total_counts, movie_genre))]

###getResult

Do some data mapping and only return the movie_name and movie_average_rating.

###getKeywordQueryResult

First I filter the data by the keywords, and then compute average rating for relevant movies. Then if the titles do not exist, return -1; if such titles are unrated, return 0. If the query succeed, return the average rating for the given keywords.

###updateResult

I first use map to let RDD contain what I need in this function(title_id, (old_rating, rating)). Then use aggregateByKey to compute the updated ratings. If the delta_ does not contain the old_rating, it means that the user give the rating for the movie for the first time, I need to increase the number of total counts and calculate the average rating by the new total counts: new_average_rating = (old_average_rating + new_rating)/new_counts. Otherwise, I need to first minus the old_rating and add the new rating while keeping the total counts the same(new_counts = counts). Then I calculate the new_average_rating = (old_average_rating -old_rating + new_rating)/new_counts. A special case is the both the old_rating and new_rating in the delta_ is null, when I need to do nothing with the previous data.

##LSH

###Hashing

Just use the Hash function provided to do a simple map operation to get the result.

###getBuckets

First hash the provided data. Then when mapping, do the minhash.hash on genre, and then reduce and convert to value into the format of List. Finally partition and cache the result.

###lookup

First get the bucket and then use left outer join to match the query.

##NNLookup

This is an extension on the LSH, just need to use the functions in class LSHIndex to compute.


##Recommender

###class BaselinePredictor

####init

I use 4 private variables to store the preprocessed data. I define an function Scale to compute the required scale.

user_avg_ratings: RDD:[(user_id, user_average_rating)]

movie_avg_ratings: RDD:[(movie_id, movie_average_rating)]

state: RDD:[(user_id, movie_id, rating, user_avg, user_avg_dev)]

The global_average is then computed and store as Float.

####predict

First, I first transform all of the null values in the user_avg_ratings into global_average and movie_avg_ratings into 0, which makes it convenient for the following processing. Then if there is no rating for m in the training set, or the movie_average_rating is 0.0, these two situations are the same and I just need to judge whether movie_average_rating equals 0.0. The goal of transforming the user_avg_rating is the same: I just need to judge whether user_avg_rating equals global_average. Finally, we make judgment and match the corresponding calculation method.


###class collaborativeFiltering

Use private variable **model** to store the machine learning model.

####init

Use the library and given parameters to build the ALS model.

####predict

make prediction through the built model.


##Recommender

###recommendBaseline

I first do the NNLookup and only keep the movie_ids. Then drop the movies that the user has seen(movies that the user has rated). Finally use BaselinePredictor to make prediction and sort the K movies with highest predicted ratings.


###recommendCollaborative

Similar to the Baseline, but replace the recommending algorithm with collaborative one.












