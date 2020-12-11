import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
object PageRank{

  def main(args: Array[String]): Unit = {

    // if argument parameter missing print the proper argument
    if (args.length != 3) {
      println("Usage: PageRank APSD -> Inputfile NoOfIterations OutputDirectory")
    }


    val sparkConf = new SparkConf().setAppName("Page Rank")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SparkSession.Builder()
      .config(sparkConf)
      .getOrCreate()

    val inputFile = args(0)
    val max_no_iterations = args(1).toInt
    val output_directory = args(2)


    import sqlContext.implicits._
    val flight_df = sqlContext.read.option("header","true").option("inferSchema","true").csv(inputFile)


    //val input_df = spark.read.option("header","true").option("inferSchema","true").csv(input_file)
    var airports_map = sparkContext.parallelize(flight_df.map(x => (x(0).toString,x(1).toString)).collect())
    val allairportsList = (airports_map.map { case (src, dest) => src }.distinct() ++ airports_map.map { case (src, dest) => dest }.distinct()).distinct().collect()

    val outLinks = airports_map.groupByKey().map(x => (x._1, x._2.size)).collect().map(x => (x._1, x._2)).toMap
    val pageRank = collection.mutable.Map() ++ allairportsList.map(airport => (airport, 10.0)).toMap

    for (i <- 1 to max_no_iterations) {
      val out = collection.mutable.Map() ++ allairportsList.map(airport => (airport, 0.0)).toMap
      pageRank.keys.foreach((id) => pageRank(id) = pageRank(id) / outLinks(id))
      for ((key, value) <- airports_map.collect()) {
        out.put(value, out(value) + pageRank(key))
      }
      val out1 = collection.mutable.Map() ++ out.map(x => (x._1, ((0.15 / allairportsList.size) + (1-0.15) * x._2)))
      out1.keys.foreach((id) => pageRank(id) = out1(id))
    }

    val result = pageRank.toSeq.sortBy(-_._2)
    sparkContext.parallelize(result).saveAsTextFile(output_directory)
  }

}