package net.tev

import net.tev.DurationSelection.{BY_AVERAGE_DURATION, BY_MAXIMUM_DURATION}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
 * This driver will manage the processing pipeline to be run inside of Spark.
 */
object WeblogAnalysis {
  val LOGGER: Logger = LoggerFactory.getLogger("TeVLog")

  def main(args: Array[String]): Unit = {
    val parsedConfig = parser.parse(args, Config())
    require(parsedConfig.nonEmpty, "Invalid parameters")
    // Parse duration selections early to fail fast in case of invalid parameter
    val durationSelections = parseDurationSelection(parsedConfig.get.durationStatsSelection)
    val userSelections = parseDurationSelection(parsedConfig.get.topUserDurationSelection)

    implicit val spark: SparkSession = SparkSession.builder().appName(parsedConfig.get.name).getOrCreate()

    LOGGER.info(s"Loading logs from ${parsedConfig.get.src}")
    val logs = new LogParser(parsedConfig.get.src).parse()
    LOGGER.info(s"Aggregating session data based on ${parsedConfig.get.timeout} seconds timeout")
    val sessions = new Sessionizer(logs).sessionize(parsedConfig.get.timeout).cache

    LOGGER.info(s"Saving session data in ${parsedConfig.get.output}")
    sessions.write.parquet(parsedConfig.get.output)

    val analysis = new SessionAnalysis(sessions)

    val stats = for {
      durationStatSelection <- durationSelections
      durationStat = analysis.durationStats(durationStatSelection)
    } yield (durationStatSelection.name, durationStat)
    val topCharts = for {
      topUserSelection <- userSelections
      topUsers = analysis.topUsers(parsedConfig.get.numUsers, topUserSelection)
    } yield (topUserSelection.name, topUsers)

    // Log requested results after jobs are finished to make it easier to find
    for (stat <- stats) {
      LOGGER.info(s"${stat._1} duration: ${stat._2}")
    }
    for (topUsers <- topCharts) {
      LOGGER.info(s"List top ${parsedConfig.get.numUsers} users based on ${topUsers._1} session duration")
      for (user <- topUsers._2) {
        LOGGER.info(s"${user.clientIp} has ${topUsers._1} session duration of ${user.duration}")
      }
    }
  }

  def parseDurationSelection(selectionParameter: String): Seq[DurationSelection] = {
    for {
      selection <- selectionParameter.split(",")
    } yield DurationSelection.durationSelectionFromName(selection)
  }

  val parser: OptionParser[Config] = new OptionParser[Config]("TeVSpark") {
    opt[String]('n', "name").required().action((n, conf) => conf.copy(name = n)).text("name of the Spark application")
    opt[String]('s', "source").required().action((s, conf) => conf.copy(src = s))
                              .text("source where to parse logs from")
    opt[String]('o', "output").required().action((o, conf) => conf.copy(output = o))
                              .text("output where to store session data")
    opt[Long]('t', "timeout").action((t, conf) => conf.copy(timeout = t)).text(
      s"timeout of inactivity, in seconds,  before session expires, defaults to $DEFAULT_SESSION_TIMEOUT")
    opt[String]('d', "durationStat").action((d, conf) => conf.copy(durationStatsSelection = d)).text(
      s"Comma separated list of statistic selection, should be in ${
        DurationSelection.supportedSelections()
      }, defaults to ${BY_AVERAGE_DURATION.name}")
    opt[String]('u', "userStat").action((u, conf) => conf.copy(topUserDurationSelection = u)).text(
      s"Comma separated list of statistic selection for top user analysis, should be in ${
        DurationSelection.supportedSelections()
      }, defaults to ${BY_MAXIMUM_DURATION.name}")
    opt[Int]('n', "numUsers").action((n, conf) => conf.copy(numUsers = n)).text(
      s"numUsers to include in top user report, defaults to 10")
  }
}

final case class Config(name: String = "",
                        src: String = "",
                        output: String = "",
                        timeout: Long = DEFAULT_SESSION_TIMEOUT,
                        durationStatsSelection: String = DurationSelection.BY_AVERAGE_DURATION.name,
                        numUsers: Int = 10,
                        topUserDurationSelection: String = DurationSelection.BY_MAXIMUM_DURATION.name)
