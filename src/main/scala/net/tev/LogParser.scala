package net.tev

import java.sql.Timestamp
import java.time.ZonedDateTime

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.matching.Regex

/**
 * The LogParser will load data from specified path, expecting the format follows the LOG_SCHEMA. It will
 * error out if the log data contains invalid records.
 *
 * @param logPath Path to log data.
 * @param spark   Spark session in which to perform processing.
 */
class LogParser(logPath: String)(implicit spark: SparkSession) {
  /**
   * @return A Dataset of Log objects. It will error out upon materialization if input data contains invalid records.
   */
  def parse(): Dataset[Log] = {
    import spark.implicits._

    // CSV format encounters too many errors with (un)escaped quotes, parse directly
    val rawLogs = spark.read.textFile(logPath)

    rawLogs.map(l => LogParser.lineToLog(l))
  }
}

object LogParser {
  /**
   * Construct a Log object from a line of textual log.
   *
   * @param line Line from log file
   * @return Log object containing data from the line.
   */
  def lineToLog(line: String): Log = {
    val pattern = "([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) (-|([^ ]*):([0-9]*)) (-?[\\.0-9]*) (-?[\\.0-9]*) (-?[\\.0-9]*) " +
      "(-|[0-9]*) " +
      "(-|[0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^: ]+)://([^: ]+):(\\d*)([^\\? ]+)\\??([^ ]*) (- |[^ ]*)\" \"" +
      "([^\"]*)\" ([^ ]*) ([^ ]*)"
    val matched: Option[Regex.Match] = pattern.r.findFirstMatchIn(line)
    require(matched.isDefined, s"Log line could not be parsed: $line")
    val time = new Timestamp(ZonedDateTime.parse(matched.get.group(1)).toEpochSecond * 1000)
    val backend = if (matched.get.group(5) == "-") {
      null
    } else {
      IpPort(matched.get.group(6),
             matched.get.group(7).toInt)
    }
    Log(time,
        matched.get.group(2),
        IpPort(matched.get.group(3),
               matched.get.group(4).toInt),
        backend,
        matched.get.group(8).toDouble,
        matched.get.group(9).toDouble,
        matched.get.group(10).toDouble,
        matched.get.group(11).toInt,
        matched.get.group(12).toInt,
        matched.get.group(13).toLong,
        matched.get.group(14).toLong,
        Request(matched.get.group(15),
                matched.get.group(16),
                matched.get.group(17),
                matched.get.group(18).toInt,
                matched.get.group(19),
                matched.get.group(20),
                matched.get.group(21)),
        matched.get.group(22),
        matched.get.group(23),
        matched.get.group(24))
  }
}
