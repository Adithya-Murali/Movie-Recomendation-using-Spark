import java.util
import org.apache.log4j._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{udf, col}
import scala.collection.mutable
import scala.math.sqrt
import scala.io.StdIn.readLine

object Recommender {
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    var startTime = System.currentTimeMillis()

    var moviePath:String = null
    var ratingPath:String = null
    if(args.length == 2){
      moviePath = args(0)
      ratingPath = args(1)
    }
    else {
      println("All required arguments have not been provided.\nPlease provide args: path_to_movies.csv path_to_ratings.csv movieID")
      sys.exit(1)
    }

    val session = SparkSession.builder
//       .master("local[4]")   //To be uncommented for local run
      .appName("MyRecommendation")
      .getOrCreate()

    import session.implicits._

//    reading movie data
    val genreArray = udf(genreArrayer _)
    val movieSchema = Encoders.product[MovieData].schema
    val movies = session.read
      .option("header","true")
      .schema(movieSchema)
      .csv(moviePath)
      .withColumn("genres",genreArray('genres))

//    reading user ratings
    val ratingSchema = Encoders.product[RatingData].schema
    val ratings = session.read
      .option("header","true")
      .schema(ratingSchema)
      .csv(ratingPath).drop("timestamp")

//    self-join ratings from same user to get all possible movie pair combinations
    val userMoviePair = ratings.as("r1").join(ratings.as("r2"),
      $"r1.userId" === $"r2.userId" && $"r1.movieId" < $"r2.movieId").drop($"r1.userId").drop($"r2.userId")

//    computing similarity between each pair of movies
    val similarMoviePair = userMoviePair.as[(Int,Double,Int,Double)]
      .groupByKey(x => (x._1,x._3))
      .mapGroups{case(key,value) =>
        val cosValue = computeCosineSimilarity(value)
        (key._1,key._2,cosValue._1,cosValue._2)
      }.toDF("movie1","movie2","score","numPairs")

    var scoreThreshold = 0.97
    var coOccurenceThreshold = 50.0
    var again:String = "N"
    do {
//      Reading movieId form user
      val movieID: Int = readLine("Enter the movieId of a movie you enjoyed: ").toInt
      println("Showing recommendation based on movie: " + movies.filter('movieId === movieID).head().getString(1))

      val genreIdentifier = udf(genreIdentityFun _)

//      joining the movie info of each movie in the pair, mainly to utilise genre information
      val movieCols = Seq("movie1", "movie2")
      val pairData = movieCols.foldLeft(similarMoviePair.where('movie1 === movieID || 'movie2 === movieID))((pair, column) =>
        pair
          .join(movies.withColumnRenamed("title", column + "title").withColumnRenamed("genres", column + "genre"),
            col(column) === $"movieId",
            "left_outer")
          .drop("movieId")
      ).withColumn("genreMatch", genreIdentifier('movie1genre, 'movie2genre)).cache

      var filteredData = pairData.filter('score > scoreThreshold && 'numPairs > coOccurenceThreshold && 'genreMatch.geq(1))
      val outer = filteredData.take(10)
      var numRecords = outer.length.toLong

//      Decreasing score threshold in loop until atleast 10 records match the criteria
      while (numRecords < 10) {
        scoreThreshold -= 0.1
        coOccurenceThreshold -= 5
        filteredData = pairData.filter('score > scoreThreshold && 'numPairs > coOccurenceThreshold && 'genreMatch.geq(1))
        numRecords = filteredData.count()
      }
//      Using head instead of limit to preserve order during foreach
      val recommendations = filteredData.sort('score.desc).head(10)
      var serial = 1
      recommendations.foreach(x => {
        print(serial + ".\t")
        if (x.getInt(0) == movieID) println(x.getString(6) + ",\tscore: " + x.getDouble(2))
        else println(x.getString(4) + ",\tscore: " + x.getDouble(2))
        serial += 1
      })
      println("\nYou waited "+(System.currentTimeMillis()-startTime)/1000+"seconds for the recommendation.")

//      recieving user input to loop
      again = readLine("\nWould you like to get recommendation based on another movieId? (Y/N): ")
      startTime = System.currentTimeMillis()
    } while(again == "Y" || again =="y")
    println("\n\tHope you enjoy the movie!")

  }
//  Function to compute the similarity between each pair of movies by computing score for each pair of ratings
  def computeCosineSimilarity(moviePair:Iterator[(Int,Double,Int,Double)]): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    moviePair.foreach(ratingPair => {

      val ratingX = ratingPair._2
      val ratingY = ratingPair._4

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    })
      val numerator: Double = sum_xy
      val denominator = sqrt(sum_xx) * sqrt(sum_yy)

      var score: Double = 0.0
      if (denominator != 0) {
        score = numerator / denominator
      }
    (score, numPairs)
  }

//  Converts the genre data into array form
  def genreArrayer(genres: String): Array[Int] = {
    val categories = Seq("Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western", "IMAX", "(no genres listed)")
    val genreData = Array.fill[Int](19)(0)
    val genarr = genres.split("\\|")
    genarr.foreach(x => {
      if(x == "(no genres listed)") util.Arrays.fill(genreData,1)
      else genreData.update(categories.indexOf(x),1)
    })
    genreData
  }

//  Performs AND operation between genres of pair of movies and returns number of intersection
  def genreIdentityFun(gen1: mutable.WrappedArray[Int],gen2: mutable.WrappedArray[Int]):Int = {
    var count:Int = 0
    gen1.foreach(x => if(x == 1 && gen2(gen1.indexOf(x)) == 1) count += 1)
    count
  }
}
//Case classes to form schema for data read
case class RatingData(userId: Int, movieId: Int, rating: Double, timestamp:String)
case class MovieData(movieId: Int, title: String, genres: String)
