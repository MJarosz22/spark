package RDDAssignment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import utils.{Commit, File, Loader, Stats}

import scala.reflect.io.Path

/**
  * This class contains the necessary boilerplate code for testing with Spark. This contains the bare minimum to run
  * Spark, change as you like! It is highly advised to write your own tests to test your solution, as it can give you
  * more insight in your solution. You can also use the
  */
class StudentTest extends FunSuite with BeforeAndAfterAll {


  //Set logger level (Warn excludes info)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.WARN)


  //  This is mostly boilerplate code, read the docs if you are interested!
  val spark: SparkSession = SparkSession
    .builder
    .config("spark.driver.host", "localhost")
    .appName("Spark-Assignment")
    .master("local[*]")
    .getOrCreate()

  implicit val sql: SQLContext = spark.sqlContext

  import spark.implicits._

  val commitDF: DataFrame = Loader.loadJSON(Path("data/data_raw.json"))
  commitDF.cache()

  val commitRDD = commitDF.as[Commit].rdd
  commitRDD.cache()

  test("Assert RDD assignment 1") {
    assertResult(10000L) {
      RDDAssignment.assignment_1(commitRDD)
    }
  }

  test("Assert RDD assignment 2") {
    val studentResult: RDD[(String, Long)] = RDDAssignment.assignment_2(commitRDD)
    val expectedSet = Set(("cmake", 13L), ("png", 4467L))
    assertResult(expectedSet) {
      studentResult.collect().toSet.intersect(expectedSet)
    }
  }


  test("Assert RDD assignment 3") {
    val studentResult: RDD[(Long, String, Long)] = RDDAssignment.assignment_3(commitRDD)
    val expected = Set((8L, "Otto Taute", 34L), (9L, "SpeedTracker", 33L), (87L, "John", 8L))
    assertResult(expected) {
      studentResult.take(96).toSet.intersect(expected)
    }
  }

  test("Assert RDD assignment 4") {
    val studentResult: RDD[(String, Stats)] = RDDAssignment.assignment_4(commitRDD, List("Otto Taute"))
    val expectedSet = Set(("Otto Taute", Stats(724, 703, 21)))
    assertResult(expectedSet) {
      studentResult.collect().toSet
    }
  }

  test("Assert RDD assignment 5") {
    val expectedSubset = Set("EdenStrive", "Paul Blackmore", "nextgis")
    assertResult(3) {
      RDDAssignment.assignment_5(commitRDD).filter(x => expectedSubset.contains(x)).count()
    }
  }

  test("Assert RDD assignment 6") {
    val studentResult: RDD[(String, (Int, Int))] = RDDAssignment.assignment_6(commitRDD.sample(false, 0.5, 0L))
    val expectedSet = Set(("Oscar Näzell", (2, 1)), ("DMEdesignmyeye", (2, 1)), ("zebbykhairah", (1, 1)),
      ("sayan7848", (1, 2)), ("Max Stewart", (1, 1)), ("Yehuda Alkalay", (1, 1)), ("Aleksander Andresen", (1, 1)),
      ("joshuous", (1, 1)), ("jalalirs", (1, 1)), ("steverendell", (1, 1)))
    assertResult(expectedSet) {
      studentResult.collect().toSet
    }
  }

  test("Assert RDD assignment 7") {
    val studentResult: RDD[(String, Long, Iterable[String])] = RDDAssignment.assignment_7(commitRDD)
    val expectedSet = Set(("SharedUmbrella", 1, List("AlbertGandolf")))
    assertResult(expectedSet) {
      studentResult.filter(x => x._1 == "SharedUmbrella").collect().map(x => (x._1, x._2, x._3.toList.sorted)).toSet
    }
  }

  test("Assert RDD assignment 8") {
    val studentResult: RDD[(String, Iterable[File])] = RDDAssignment.assignment_8(commitRDD)
    val expectedSet =
      File(Some("eea08e79ef5ba079af92e5eebad9239bd128213e"), Some("src/style.css"),
        Some("modified"), 6, 2, 8,
        Some("https://github.com/javie-lau/hackaton-pelicula-009/blob/c54d71714512a303d3ad33f489c774d552782e14/src/style.css"),
        Some("https://github.com/javie-lau/hackaton-pelicula-009/raw/c54d71714512a303d3ad33f489c774d552782e14/src/style.css"),
        Some("https://api.github.com/repos/javie-lau/hackaton-pelicula-009/contents/src/style.css?ref=c54d71714512a303d3ad33f489c774d552782e14"),
        None)
    // Note that the last attribute is omitted in favour of readability. You can manually check this.
    val fileArray = studentResult.filter(x => x._1 == "hackaton-pelicula-009").collect
    val (_, fileList) = fileArray.head
    assertResult(expectedSet.sha) {
      fileList.filter((x: File)=> x.filename == expectedSet.filename).head.sha
    }
  }


  test("Assert RDD assignment 9") {
    val studentResult: RDD[(String, Seq[String], Stats)] = RDDAssignment.assignment_9(commitRDD, "gitlab-rs")
    val x: Map[String, (String, Seq[String], Stats)] = studentResult.collect().map(x => (x._1, x)).toMap
    assertResult(40) {
      x.size
    }
    assertResult(Stats(12, 12, 0)) {
      x("rustfmt.toml")._3
    }
  }

  test("Assert RDD assignment 10") {
    val studentResult: RDD[(String, String, Option[Stats])] = RDDAssignment.assignment_10(commitRDD)
    val expectedSet: Set[(String, String, Option[Stats])] = Set(("GitHub", "danfe", Some(Stats(0, 0, 0))),
      ("ivankovskiy-pc", "AIM-Chat", Some(Stats(75, 45, 30))))
    assertResult(expectedSet) {
      studentResult.filter(expectedSet.contains).collect().toSet
    }
  }


  test("Assert RDD assignment 11") {
    val studentResult: Graph[(String, String), String] = RDDAssignment.assignment_11(commitRDD);
    assertResult((5722, 9147)) {
      (studentResult.edges.count(), studentResult.vertices.count())
    }
  }

  override def afterAll(): Unit = {
    //    Uncomment the line beneath if you want to inspect the Spark GUI in your browser, the url should be printed
    //    in the console during the start-up of the driver.
    //    Thread.sleep(9999999)
    //    You can uncomment the line below. Doing so will cause errors with `maven test`
    //    spark.close()
  }

}