package DataFrameAssignment

import org.apache.spark.sql.functions.{array, array_contains, coalesce, col, desc, explode, lag, lit, row_number, to_date, to_json, udf, when}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoder, Encoders}
import java.sql.Timestamp


/**
  * Please read the comments carefully, as they describe the expected result and may contain hints in how
  * to tackle the exercises. Note that the data that is given in the examples in the comments does
  * reflect the format of the data, but not the result the graders expect (unless stated otherwise).
  */
object DFAssignment {

  /**
    * In this exercise we want to know all the commit SHA's from a list of committers. We require these to be
    * ordered according to their timestamps in the following format:
    *
    * | committer      | sha                                      | timestamp            |
    * |----------------|------------------------------------------|----------------------|
    * | Harbar-Inbound | 1d8e15a834a2157fe7af04421c42a893e8a1f23a | 2019-03-10T15:24:16Z |
    * | ...            | ...                                      | ...                  |
    *
    * Hint: Try to work out the individual stages of the exercises. This makes it easier to track bugs, and figure out
    * how Spark DataFrames and their operations work. You can also use the `printSchema()` function and `show()`
    * function to take a look at the structure and contents of the DataFrames.
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @param authors Sequence of Strings representing the authors from which we want to know their respective commit
    *                SHA's.
    * @return DataFrame of commits from the requested authors, including the commit SHA and the according timestamp.
    */
  def assignment_12(commits: DataFrame, authors: Seq[String]): DataFrame = {
    val df :DataFrame = commits.filter(commits("commit.committer.name").isInCollection(authors))
    val res: DataFrame = df.select("commit.committer.name","sha","commit.committer.date").sort("commit.committer.date")
    res
  }

  /**
    * In order to generate weekly dashboards for all projects, we need the data to be partitioned by weeks. As projects
    * can span multiple years in the data set, care must be taken to partition by not only weeks but also by years.
    *
    * Expected DataFrame example:
    *
    * | repository | week             | year | count   |
    * |------------|------------------|------|---------|
    * | Maven      | 41               | 2019 | 21      |
    * | .....      | ..               | .... | ..      |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
    * @return DataFrame containing 4 columns: repository name, week number, year and the number of commits for that
    *         week.
    */
  def assignment_13(commits: DataFrame): DataFrame = {
    val getRepName = (url: String) => {
      url.split('/')(5);
    }

    val getWeek = (date: Timestamp) => {
      val time = date.getTime
      val week = math.floor((time % 3.154e+10) / 6.048e+8).longValue()
      week
    }

    val getYear = (date: Timestamp) => {
      val time = date.getTime
      val year = (math.floor((time / 6.048e+8) / 52) + 1970).longValue()
      year
    }

    val getRepNameUdf = udf(getRepName)
    val getWeekUdf = udf(getWeek)
    val getYearUdf = udf(getYear)

    val df = commits.select(getRepNameUdf(col("url")).as("repository"),
      getWeekUdf(col("commit.committer.date")).as("week"),
      getYearUdf(col("commit.committer.date")).as("year"))
    val test = df.groupBy(df("repository"),df("week"),df("year")).count()
    test
  }

  /**
    * A developer is interested in the age of commits in seconds. Although this is something that can always be
    * calculated during runtime, this would require us to pass a Timestamp along with the computation. Therefore, we
    * require you to **append** the input DataFrame with an `age` column of each commit in *seconds*.
    *
    * Hint: Look into SQL functions for Spark SQL.
    *
    * Expected DataFrame (column) example:
    *
    * | age    |
    * |--------|
    * | 1231   |
    * | 20     |
    * | ...    |
    *
    * @param commits Commit DataFrame, created from the data_raw.json file.
    * @return the input DataFrame with the appended `age` column.
    */
  def assignment_14(commits: DataFrame, snapShotTimestamp: Timestamp): DataFrame = {
    val getSeconds = (date1: Timestamp) => {
      val age = snapShotTimestamp.getTime - date1.getTime
      age / 1000
    }
    val getSecondsUdf = udf(getSeconds)
    commits.withColumn("age", getSecondsUdf(col("commit.committer.date")))
  }

  /**
    * To perform the analysis on commit behavior, the intermediate time of commits is needed. We require that the DataFrame
    * that is given as input is appended with an extra column. This column should express the number of days there are between
    * the current commit and the previous commit of the user, independent of the branch or repository.
    * If no commit exists before a commit, the time difference in days should be zero.
    * **Make sure to return the commits in chronological order**.
    *
    * Hint: Look into Spark SQL's Window to have more expressive power in custom aggregations.
    *
    * Expected DataFrame example:
    *
    * | $oid                     	| name   	| date                     	| time_diff 	|
    * |--------------------------	|--------	|--------------------------	|-----------	|
    * | 5ce6929e6480fd0d91d3106a 	| GitHub 	| 2019-01-27T07:09:13.000Z 	| 0         	|
    * | 5ce693156480fd0d5edbd708 	| GitHub 	| 2019-03-04T15:21:52.000Z 	| 36        	|
    * | 5ce691b06480fd0fe0972350 	| GitHub 	| 2019-03-06T13:55:25.000Z 	| 2         	|
    * | ...                      	| ...    	| ...                      	| ...       	|
    *
    * @param commits    Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                   `println(commits.schema)`.
    * @param authorName Name of the author for which the result must be generated.
    * @return DataFrame with an appended column expressing the number of days since last commit, for the given user.
    */
  def assignment_15(commits: DataFrame, authorName: String): DataFrame = {
    val getMS  = (date: Timestamp) => {
      date.getTime / 86400000
    }
    val getMSUdf = udf(getMS)

    val authorCommits = commits.where(commits("commit.committer.name") === authorName)
    val res = authorCommits.select(col("_id"),col("commit.committer.name"),col("commit.committer.date")).sort(col("date"))


    val td = res.withColumn("time_diff", getMSUdf(col("date")))
    val w = Window.orderBy(col("date"))
    val maybe = td.withColumn("time_diff", col("time_diff")-lag(col("time_diff"), 1, 0).over(w))
    .withColumn("row", row_number.over(w))
      .withColumn("time_diff", when(col("row") === 1, lit(0)).otherwise(col("time_diff"))).drop("row")
    maybe

  }


  /**
    * To get a bit of insight into the spark SQL and its aggregation functions, you will have to implement a function
    * that returns a DataFrame containing a column `day` (int) and a column `commits_per_day`, based on the commits'
    * dates. Sunday would be 1, Monday 2, etc.
    *
    * Expected DataFrame example:
    *
    * | day | commits_per_day|
    * |-----|----------------|
    * | 1   | 32             |
    * | ... | ...            |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing a `day` column and a `commits_per_day` column representing the total number of
    *         commits that have been made on that week day.
    */
  def assignment_16(commits: DataFrame): DataFrame = {
    val getDay = (date: Timestamp) => {
      val day = ((math.floor(date.getTime / 86400000) % 7) + 5).toInt
      if (day > 7) {
        0 + day - 7;
      } else {
        day;
      }
    }
    val getDayUdf = udf(getDay);
    val ret = commits
      .groupBy(getDayUdf(commits("commit.committer.date")).as("day")).count()
      .select(col("day"), col("count").as("commits_per_day"));
    ret;
  }

  /**
    * Commits can be uploaded on different days. We want to get insight into the difference in commit time of the author and
    * the committer. Append to the given DataFrame a column expressing the difference in *the number of seconds* between
    * the two events in the commit data.
    *
    * Expected DataFrame (column) example:
    *
    * | commit_time_diff |
    * |------------------|
    * | 1022             |
    * | 0                |
    * | ...              |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return the original DataFrame with an appended column `commit_time_diff`, containing the time difference
    *         (in number of seconds) between authorizing and committing.
    */
  def assignment_17(commits: DataFrame): DataFrame = {
    val getSeconds = (date: Timestamp) => {
      date.getTime / 1000
    }
    val getSecondsUdf = udf(getSeconds)
    commits.withColumn("commit_time_diff", getSecondsUdf(col("commit.committer.date") ) - getSecondsUdf(col("commit.author.date")))
  }

  /**
    * Using DataFrames, find all the commit SHA's from which a branch has been created, including the number of
    * branches that have been made. Only take the SHA's into account if they are also contained in the DataFrame.
    *
    * Note that the returned DataFrame should not contain any commit SHA's from which no new branches were made, and it should
    * not contain a SHA which is not contained in the given DataFrame.
    *
    * Expected DataFrame example:
    *
    * | sha                                      | times_parent |
    * |------------------------------------------|--------------|
    * | 3438abd8e0222f37934ba62b2130c3933b067678 | 2            |
    * | ...                                      | ...          |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
   *                d7ca419884ec824221c5766f5df64aea237cafd9
   *                441ca2da3b4fe9c7220288b446e0b65df4dccbc5
    * @return DataFrame containing the commit SHAs from which at least one new branch has been created, and the actual
    *         number of created branches
    */
  def assignment_18(commits: DataFrame): DataFrame = {
    val shas: DataFrame = commits.select(col("sha").as("parent")) // all the shas in the dataframe
    val shasList = shas.map(f => f.getString(0))(Encoders.STRING).collect().toSeq // list of all the shas in the DF
    val parents: DataFrame = commits.select(explode(col("parents")).as("sha"))
      .select( col("sha.*"))
      .filter(col("sha").isInCollection(shasList)) // all the parents that are in the DF
    val res = parents.groupBy("sha").count().select(col("sha"), col("count").as("times_parent")).filter(col("times_parent") > 1)
    res.show(100000, false)
    res
  }

  /**
    * In the given commit DataFrame, find all commits from which a fork has been created. We are interested in the names
    * of the (parent) repository and the subsequent (child) fork (including the name of the repository owner for both),
    * the SHA of the commit from which the fork has been created (parent_sha) as well as the SHA of the first commit that
    * occurs in the forked branch (child_sha).
    *
    * Expected DataFrame example:
    *
    * | repo_name            | child_repo_name     | parent_sha           | child_sha            |
    * |----------------------|---------------------|----------------------|----------------------|
    * | ElucidataInc/ElMaven | saifulbkhan/ElMaven | 37d38cb21ab342b17... | 6a3dbead35c10add6... |
    * | hyho942/hecoco       | Sub2n/hecoco        | ebd077a028bd2169d... | b47db8a9df414e28b... |
    * | ...                  | ...                 | ...                  | ...                  |
    *
    * Note that this example is based on _real_ data, so you can verify the functionality of your solution, which might
    * help during the debugging of your solution.
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the parent and child repository names, the SHA of the commit from which a new fork
    *         has been created and the SHA of the first commit in the fork repository
    */
  def assignment_19(commits: DataFrame): DataFrame = {
    val getRepName = (url: String) => {
      url.split('/')(4)+"/"+url.split('/')(5)
    }
    val getRepNameUdf = udf(getRepName)

    val forked :DataFrame = commits.select(getRepNameUdf(col("url")).as("child_repo_name"),
      explode(col("parents")).as("parent_sha"),
      col("sha").as("child_sha"))

    val newForked = forked.select("child_repo_name", "parent_sha.*","child_sha")
      .select(col("child_repo_name"), col("child_sha"), col("sha").as("parent_sha"))

    val repoSha = commits.select(getRepNameUdf(col("url")).as("repo_name"), col("sha").as("dummy"))
    val res = newForked.join(repoSha, newForked("parent_sha") === repoSha("dummy"),"inner")
      .select("repo_name","child_repo_name","parent_sha","child_sha").filter(col("repo_name").notEqual(col("child_repo_name")) )
    res.show(false)
    res
  }
}
