package RDDAssignment

import java.util.UUID
import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

object RDDAssignment {


  /**
   * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
   * we want to know how many commits a given RDD contains.
   *
   * @param commits RDD containing commit data.
   * @return Long indicating the number of commits in the given RDD.
   */
  def assignment_1(commits: RDD[Commit]): Long = {
    commits.count()
  }

  /**
   * We want to know how often programming languages are used in committed files. We want you to return an RDD containing Tuples
   * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
   * assume the language to be 'unknown'.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
   */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = {
    val known = commits.flatMap(c => c.files)
      .map(file => file.filename.getOrElse(".unknown"))
      .map(file => unknownCheck(file))
      .groupBy(file => file.split('.')(file.split('.').length-1))
      .map(x=>( x._1,x._2.size.longValue()))
    known

  }

  def unknownCheck(str: String): String = {
    if(!str.contains('.'))
      return ".unknown"
    str
  }

  /**
   * Competitive users on GitHub might be interested in their ranking in the number of commits. We want you to return an
   * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit author's name and the number of
   * commits made by the commit author. As in general with performance rankings, a higher performance means a better
   * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
   * tie.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing the rank, the name and the total number of commits for every author, in the ordered fashion.
   */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = {
    val res =  commits
      .groupBy(c => c.commit.author.name).map(x => (x._1, x._2.size))
      .sortBy(x => x._1.toLowerCase())
      .sortBy(x => -x._2)
      .zipWithIndex()
      .map(x => (x._2, x._1._1, x._1._2.longValue()))
    //TODO: tie breaking ???
    val test = res.collect()
    res
  }

  /**
   * Some users are interested in seeing an overall contribution of all their work. For this exercise we want an RDD that
   * contains the committer's name and the total number of their commit statistics. As stats are Optional, missing Stats cases should be
   * handled as "Stats(0, 0, 0)".
   *
   * Note that if a user is given that is not in the dataset, then the user's name should not occur in
   * the resulting RDD.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing committer names and an aggregation of the committers Stats.
   */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {
    commits
      .filter(c => users.contains(c.commit.committer.name))
      .groupBy(c => c.commit.committer.name).map(x =>(x._1, x._2.map(x => x.stats.getOrElse(new Stats(0,0,0)))))
      .map(x => (x._1, x._2.aggregate(new Stats(0,0,0))((acc,x) => new Stats(acc.total + x.total, acc.additions + x.additions, acc.deletions+x.deletions),
        (x,y)=>(new Stats(x.total+y.total, x.additions + y.additions, x.deletions + y.deletions)))))
  }


  /**
   * There are different types of people: those who own repositories, and those who make commits. Although Git blame command is
   * excellent in finding these types of people, we want to do it in Spark. As the output, we require an RDD containing the
   * names of commit authors and repository owners that have either exclusively committed to repositories, or
   * exclusively own repositories in the given commits RDD.
   *
   * Note that the repository owner is contained within GitHub URLs.
   *
   * @param commits RDD containing commit data.
   * @return RDD of Strings representing the usernames that have either only committed to repositories or only own
   *         repositories.
   */
  def assignment_5(commits: RDD[Commit]): RDD[String] = {
    val owners = commits.map(c => c.url.split('/')(4)).distinct()
    val authors = commits.map(c => c.commit.author.name).distinct()
    owners.subtract(authors).union(authors.subtract(owners))
  }

  /**
   * Sometimes developers make mistakes and sometimes they make many many of them. One way of observing mistakes in commits is by
   * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
   * in a commit message. Note that for a commit to be eligible for a 'revert streak', its message must start with `Revert`.
   * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
   * would not be a 'revert streak' at all.
   *
   * We require an RDD containing Tuples of the username of a commit author and a Tuple containing
   * the length of the longest 'revert streak' of a user and how often this streak has occurred.
   * Note that we are only interested in the longest commit streak of each author (and its frequency).
   *
   * @param commits RDD containing commit data.
   * @return RDD of Tuples containing a commit author's name and a Tuple which contains the length of the longest
   *         'revert streak' as well its frequency.
   */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = {
    commits.groupBy(x => x.commit.author.name)
      .map(x => (x._1, x._2.map(y => y.commit.message)))
      .map(x => (x._1, x._2.map(y => getRevertStreak(y))))
      .map(x => (x._1, x._2.aggregate(0,0)((acc,a)=>aggF(acc,a),(a,b)=>aggP(a,b))))
      .filter(x => x._2._1 != 0)
  }

  def aggF(acc: (Int, Int), tup: Int):(Int, Int) = {
    if(tup==acc._1)
      return (acc._1,acc._2+1)
    if(tup>acc._1)
      return(tup,1)
    return acc
  }

  def aggP(tuple1: (Int, Int), tuple2: (Int, Int)):(Int,Int) = {
    if(tuple1._1 > tuple2._1)
      return tuple1
    if (tuple1._1 < tuple2._1)
      return tuple2
    return(tuple1._1, tuple1._2 + tuple2._2)
  }

  def getRevertStreak(str: String):(Int) = {
    if(!str.startsWith("Revert"))
      return 0
    str.split(' ').filter(x => x.contains("Revert")).size
  }


  /**
   * !!! NOTE THAT FROM THIS EXERCISE ON (INCLUSIVE), EXPENSIVE FUNCTIONS LIKE groupBy ARE NO LONGER ALLOWED TO BE USED !!
   *
   * We want to know the number of commits that have been made to each repository contained in the given RDD. Besides the
   * number of commits, we also want to know the unique committers that contributed to each of these repositories.
   *
   * In real life these wide dependency functions are performance killers, but luckily there are better performing alternatives!
   * The automatic graders will check the computation history of the returned RDDs.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing Tuples with the repository name, the number of commits made to the repository as
   *         well as the names of the unique committers to this repository.
   */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = {
    commits.map(c => (c.url.split('/')(5), c.commit.committer.name))
      .aggregateByKey((List[String](), 0))((a, v) => ((v :: a._1).distinct, a._2 + 1), (a1, a2) => (a1._1 ::: a2._1, a1._2 + a2._2))
      .map(x => (x._1, x._2._2.longValue(), x._2._1.toIterable));
  }

  /**
   * Return an RDD of Tuples containing the repository name and all the files that are contained in this repository.
   * Note that the file names must be unique, so if a file occurs multiple times (for example, due to removal, or new
   * addition), the newest File object must be returned. As the filenames are an `Option[String]`, discard the
   * files that do not have a filename.
   *
   * To reiterate, expensive functions such as groupBy are not allowed.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing the files in each repository as described above.
   */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = {
    /*val res = commits.map(c => (c.url.split('/')(5),
      c.files.aggregate(List[File]())((acc,x: File)=> fileAggregator(acc,x), (a,b) => listAggregator(a, b) ).filter(f => f.filename.isDefined).toIterable))
    res*/
    val res = commits.keyBy(c => c.url.split('/')(5))
      .aggregateByKey(List(): List[File])((acc: List[File], x)=> listAggregator(acc,x.files.filter(x => x.filename.isDefined)), (a, b) => listAggregator(a,b))
      .map(x => (x._1, x._2.toIterable))
    //HOW TO FIND NEWEST FILE OBJECT???
    res
  }

  def fileAggregator(acc: List[File], x: File):File = {
    if(acc.map(file => file.filename).contains(x.filename)){
      val oldFile = acc.filter(f => f.filename==x.filename)
      if (oldFile(0).changes < x.changes)
        return oldFile(0)
    }
    x
  }

  def listAggregator(a: List[File], b: List[File]):List[File] = {
    val commonFiles = a.map(f=>f.filename).intersect(b.map(f=>f.filename))
    if(commonFiles.nonEmpty) {
      val placeHolder = "hi"
    }
    val filesA = a.filter(x => !commonFiles.contains(x.filename))
    val temp = b.map(x => fileAggregator(a,x)).toList
    temp ::: filesA
  }


  /**
   * For this assignment you are asked to find all the files of a single repository. This is in order to create an
   * overview of each file by creating a Tuple containing the file name, all corresponding commit SHA's,
   * as well as a Stat object representing all the changes made to the file.
   *
   * To reiterate, expensive functions such as groupBy are not allowed.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
   *         representing the total aggregation of changes for a file.
   */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = {
    val ret = commits.filter(c => c.url.split('/')(5) == repository).flatMap(c => c.files).filter(f => f.filename.isDefined && f.sha.isDefined)
      .map(f => (f.filename.get, f))
      .aggregateByKey((Seq[String](), Stats(0, 0, 0)))((a, v) =>
        ((a._1 :+ v.sha.get), (Stats(a._2.total + v.deletions + v.additions, a._2.additions + v.additions, a._2.deletions + v.deletions))),
        (a1, a2) => ((a1._1 ++ a2._1), (Stats(a1._2.total + a2._2.total, a1._2.additions + a2._2.additions, a1._2.deletions + a2._2.deletions))))
      .map(x => (x._1, x._2._1.toSeq, x._2._2));
    ret;
  }

  /**
   * We want to generate an overview of the work done by a user per repository. For this we want an RDD containing
   * Tuples with the committer's name, the repository name and a `Stats` object containing the
   * total number of additions, deletions and total contribution to this repository.
   * Note that since Stats are optional, the required type is Option[Stat].
   *
   * To reiterate, expensive functions such as groupBy are not allowed.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing Tuples of the committer's name, the repository name and an `Option[Stat]` object representing additions,
   *         deletions and the total contribution to this repository by this committer.
   */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = {
    val res = commits.keyBy(c => (c.commit.committer.name, c.url.split('/')(5)))
      .aggregateByKey(None: Option[Stats])((acc: Option[Stats], x) => aggregator(acc, x.stats),
      (x: Option[Stats], y: Option[Stats]) => aggregator(x, y))
      .map(c => (c._1._1, c._1._2, c._2))
    res
  }
  def aggregator(acc: Option[Stats], x: Option[Stats]): Option[Stats] = {
    if(acc.isDefined || x.isDefined) {
      val s1 = acc.getOrElse(Stats(0,0,0))
      val s2 = x.getOrElse(Stats(0,0,0))
      return Option(addStats(s1,s2))
    }
    None
  }

  def addStats(s1: Stats, s2: Stats): Stats = {
    Stats(s1.total + s2.total,s1.additions + s2.additions, s1.deletions + s2.deletions)
  }

  /**
   * Hashing function that computes the md5 hash of a String and returns a Long, representing the most significant bits of the hashed string.
   * It acts as a hashing function for repository name and username.
   *
   * @param s String to be hashed, consecutively mapped to a Long.
   * @return Long representing the MSB of the hashed input String.
   */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
   * Create a bi-directional graph from committer to repositories. Use the `md5HashString` function above to create unique
   * identifiers for the creation of the graph.
   *
   * Spark's GraphX library is actually used in the real world for algorithms like PageRank, Hubs and Authorities, clique finding, etc.
   * However, this is out of the scope of this course and thus, we will not go into further detail.
   *
   * We expect a node for each repository and each committer (based on committer name).
   * We expect an edge from each committer to the repositories that they have committed to.
   *
   * Look into the documentation of Graph and Edge before starting with this exercise.
   * Your vertices must contain information about the type of node: a 'developer' or a 'repository' node.
   * Edges must only exist between repositories and committers.
   *
   * To reiterate, expensive functions such as groupBy are not allowed.
   *
   * @param commits RDD containing commit data.
   * @return Graph representation of the commits as described above.
   */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] = {
    val develepors = commits.map(c => c.commit.committer.name).map(x => (x.##.longValue(), (x, "develeper")));
    val repositories = commits.map(c => (c.url.split('/')(4) + "/" + c.url.split('/')(5), 1))
      .aggregateByKey(0)((a, v) => (a + 1), (a1, a2) => (a1 + a2)).map(x => (x._1.##.longValue(), (x._1, "repository")));
    val users = develepors ++ repositories;
    val relationships = commits.map(c => (Edge(c.commit.committer.name.##, (c.url.split('/')(4) + "/" + c.url.split('/')(5)).##, "commit"), 1))
      .aggregateByKey(0)((a, v) => (a + 1), (a1, a2) => (a1 + a2)).map(x => x._1);;
    val defaultUser = ("John Doe", "Missing")
    Graph(users, relationships, defaultUser);
  }
}
