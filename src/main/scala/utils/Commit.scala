package utils

import java.sql.Timestamp

/*
 * Note that these classes are sealed, much like final in Java. This means that you cannot create extensions of these
 * classes outside of the file where they are defined. You can create extensions of these classes, but the grader will
 * not take the code you put in this file into account, as it only assesses the code in the `src.main.scala.*` folders.
 */

sealed case class Commit(_id: String,
                  node_id: String,
                  sha: String,
                  url: String,
                  commit: CommitData,
                  author: Option[User],
                  committer: Option[User],
                  parents: List[Parent],
                  stats: Option[Stats],
                  files: List[File])

sealed case class CommitData(author: CommitUser,
                      committer: CommitUser,
                      message: String,
                      tree: Tree,
                      comment_count: Long,
                      verification: Verification)

sealed case class CommitUser(name: String, email: String, date: Timestamp)

sealed case class User(id: Long,
                login: String,
                avatar_url: String,
                `type`: String,
                site_admin: Boolean)

sealed case class Verification(verified: Boolean,
                        reason: String)

sealed case class Stats(total: Long, additions: Long, deletions: Long)

sealed case class File(sha: Option[String],
                filename: Option[String],
                status: Option[String],
                additions: Long,
                deletions: Long,
                changes: Long,
                blob_url: Option[String],
                raw_url: Option[String],
                contents_url: Option[String],
                patch: Option[String])

sealed case class Parent(sha: String)

sealed case class Tree(sha: String)