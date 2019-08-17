package net.tev

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * The Sessionizer groups requests by session, where a session is defined as a series of requests from the same
 * IP address (ignoring port) with no period of inactivity longer than a specified timeout.
 *
 * @param logs  Logs to group by session.
 * @param spark Spark session in which to perform computation.
 */
class Sessionizer(logs: Dataset[Log])(implicit spark: SparkSession) {
  /**
   * Group user activity by session.
   *
   * @param sessionTimeout Maximum length of inactivity period before a session expires.
   * @return User activity grouped by session, where the sessionId is an incremental number representing the number of
   *         sessions a given user has had. Both unique hits as well as full list of page hits are included in the
   *         session information returned, as well as the duration, in seconds, of every session.
   */
  def sessionize(sessionTimeout: Long = DEFAULT_SESSION_TIMEOUT): Dataset[Session] = {
    import spark.implicits._
    val logsWithSessionInfo = logs.withColumn("clientIp", $"client.ip")
                                  // Add timestamp of previous activity from this IP
                                  .withColumn("previousTime",
                                              lag($"time", 1).over(Window.partitionBy($"clientIp").orderBy($"time")))
                                  // If last activity was too far behind, consider this a new session
                                  .withColumn("newSession",
                                              when(unix_timestamp($"time")
                                                     .minus(unix_timestamp($"previousTime")) < lit(sessionTimeout), 0)
                                                .otherwise(1))
                                  // Assign an incremental session id based on number of new session the user had so far
                                  .withColumn("sessionId", sum($"newSession")
                                    .over(Window.partitionBy($"clientIp").orderBy($"clientIp", $"time")))

    logsWithSessionInfo.groupBy($"clientIp", $"sessionId")
                       // Aggregate page hits both as full list and set of unique hits
                       .agg(collect_list($"request.path").as("hits"),
                            collect_set($"request.path").as("uniques"),
                            // Duration of the session as last activity time minus first
                            max(unix_timestamp($"time")).minus(min(unix_timestamp($"time"))).as("duration"))
                       .as[Session]
  }
}
