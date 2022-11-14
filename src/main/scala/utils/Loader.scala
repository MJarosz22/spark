package utils

import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.reflect.io.Path

object Loader {


  /**
    * Function for loading a DataFrame from a Scala IO Path. Can be used in test classes to easily generate a DataFrame
    * from a json file. Its setup allows the json to be split over multiple lines (allowing both minified and
    * pretty-print JSON).
    *
    * @param path Path to file which contains the JSON file, you should not have to change this.
    * @return DataFrame generated from the JSON file, Spark will automatically generate a Schema inferred from the JSON
    *         that was provided.
    */
  def loadJSON(path: Path)(implicit sql: SQLContext): DataFrame = {
    sql.read.option("multiLine", "true").format("json").json(path.path)
  }


}
