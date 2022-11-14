package DataFrameAssignment

import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp
import org.apache.spark.sql.functions.{col, isnull}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import utils.{Commit, Loader}

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

  test("Assert DF assignment 12") {
    val studentResult = DFAssignment.assignment_12(commitDF, List("GitHub"))
    val expectedSet = List(("GitHub", "65a88dce81af6d4dc6751deab202fa8eeadaf9c0", "2019-01-27T07:09:13.000Z"),
      ("GitHub", "54fc3ef7d48b1c25910485b8f369023077d0f995", "2019-03-04T15:21:52.000Z"))
    assertResult(expectedSet) {
      studentResult.as[(String, String, String)].take(2).toList
    }
  }

  test("Assert DF assignment 13") {
    val studentResult = DFAssignment.assignment_13(commitDF)
    val expectedSet: Set[(String, Long, Long, BigInt)] = Set(("DrTests", 21, 2019, 1), ("ExoGit", 21, 2019, 6))
    assertResult(2) {
      studentResult.as[(String, Long, Long, BigInt)].filter(x => expectedSet.contains(x)).count()
    }
  }

  test("Assert DF assignment 14") {
    val time = 1570707029000L

    val studentResult = DFAssignment.assignment_14(commitDF, new Timestamp(time))
    val expectedSet: List[Long] = List(12093031, 12093019, 12093032, 12093030)
    assertResult(expectedSet) {
      studentResult.take(4).map(x => x(10)).toList
    }
  }

  test("Assert DF assignment 15") {
    val studentResult = DFAssignment.assignment_15(commitDF, "GitHub")
    val expectedSet = Set(
      ("count", "2532"),
      ("stddev", "0.8288280854699502"),
      ("max", "36"),
      ("min", "0"),
      ("mean", "0.045813586097946286")
    )
    assertResult(expectedSet) {
      studentResult.describe("time_diff").as[(String, String)].collect().toSet
    }
  }


  test("Assert DF assignment 16") {
    val studentResult = DFAssignment.assignment_16(commitDF.sample(0.2, 0L))
    val expectedSet: Set[(Int, BigInt)] = Set((7,7), (4,71), (3,34), (1,9), (2,31), (5,1816), (6,20))
    assertResult(expectedSet) {
      studentResult.as[(Int, BigInt)].rdd.collect().toSet
    }
  }

  test("Assert DF assignment 17") {
    val studentResult = DFAssignment.assignment_17(commitDF.sample(0.2, 0L))
    val expectedSet = Set(
      ("count", "1988"),
      ("stddev", "941058.3612074541"),
      ("max", "32900181"),
      ("min", "0"),
      ("mean", "68532.0226358149")
    )
    assertResult(0L) {
      studentResult.filter(isnull(col("commit_time_diff"))).count()
    }
    assertResult(expectedSet) {
      studentResult.describe("commit_time_diff").as[(String, String)].collect().toSet
    }
  }

  test("Assert DF assignment 18") {
    val studentResult = DFAssignment.assignment_18(commitDF)
    val expectedSet = Set(("b27fb34c3f5c3e9a4e4deb969eedf82d810884aa", 2L),
      ("46c9906f39b7e8938338693f7446a0af06e8a211", 2L))
    assertResult(expectedSet) {
      studentResult.as[(String, Long)].collect().toSet.intersect(expectedSet)
    }
  }

  test("Assert DF assignment 19") {
    val studentResult = DFAssignment.assignment_19(commitDF)
    val expectedSet = Set(("NantesJS/nantesjs-website", "jtanguy/nantesjs-website", "4c97f72aab501592f9be0f87e68c18b59af12331", "7a4ba24c8734890117e2ec104126fa51db21edc8"),
      ("shengcui2018/qcloud-documents", "tencentyun/qcloud-documents", "bc71960acafe0a182cf15373fdb83da353bea8be", "094e04fffee23fae3381ca007ef88a380c912bd9"))
    assertResult(expectedSet) {
      studentResult.as[(String, String, String, String)].collect().toSet.intersect(expectedSet)
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