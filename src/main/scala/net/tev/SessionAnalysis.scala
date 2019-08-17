package net.tev

import net.tev.DurationSelection.BY_AVERAGE_DURATION
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * The SessionAnalysis extracts statistical information from sessions.
 *
 * @param sessions Sessions to analyze.
 * @param spark    Spark session in which to perform analysis.
 */
class SessionAnalysis(sessions: Dataset[Session])(implicit spark: SparkSession) {
  /**
   * Find the top users according to specified duration selection mechanism.
   *
   * @param num       Number of users to return (defaults to 10).
   * @param selection Selection mechanism (defaults to average)
   * @return Top X users with corresponding duration statistic.
   */
  def topUsers(num: Int = 10, selection: DurationSelection = BY_AVERAGE_DURATION): Seq[UserDurationStats] = {
    import spark.implicits._
    val clientToDuration = sessions.groupBy($"clientIp").agg(selection.select($"duration").as("duration"))
                                   .orderBy($"duration".desc).as[UserDurationStats]
    clientToDuration.take(num)
  }

  /**
   * Compute global session duration statistics.
   *
   * @param selection Duration selection mechanism (defaults to average).
   * @return Minimum, average or maximum session duration, depending on selection mechanism.
   */
  def durationStats(selection: DurationSelection = BY_AVERAGE_DURATION): Double = {
    import spark.implicits._
    sessions.agg(selection.select($"duration").cast(DoubleType)).first().getAs[Double](0)
  }
}

/**
 * Representation of user statistics.
 *
 * @param clientIp IP address used to identify user.
 * @param duration Computed duration for this user, depends on selected DurationSelection.
 */
final case class UserDurationStats(clientIp: String, duration: Double)
