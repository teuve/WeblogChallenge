package net

import java.sql.Timestamp

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.concurrent.duration._

/**
 * Package object for utility case classes, constants and schemas
 */
package object tev {
  val DEFAULT_SESSION_TIMEOUT: Long = 15.minutes.toSeconds

  final case class Log(time: Timestamp,
                       elb: String,
                       client: IpPort,
                       backend: IpPort,
                       requestProcessingTime: Double,
                       backendProcessingTime: Double,
                       responseProcessingTime: Double,
                       elbStatusCode: Int,
                       backendStatusCode: Int,
                       receivedBytes: Long,
                       sentBytes: Long,
                       request: Request,
                       userAgent: String,
                       sslCipher: String,
                       sslProtocol: String)

  final case class Session(clientIp: String,
                           sessionId: Long,
                           hits: Seq[String],
                           uniques: Set[String],
                           duration: Long)

  final case class IpPort(ip: String,
                          port: Int)

  final case class Request(method: String,
                           scheme: String,
                           host: String,
                           port: Int,
                           path: String,
                           query: String,
                           version: String)

  /**
   * Represents different options of selecting duration information for users.
   */
  sealed trait DurationSelection {
    def select: Column => Column

    def name: String
  }

  object DurationSelection {
    def supportedSelections(): Seq[String] = Seq(BY_MINIMUM_DURATION.name, BY_MAXIMUM_DURATION.name,
                                                 BY_AVERAGE_DURATION.name, BY_COUNT_DURATION.name,
                                                 BY_TOTAL_DURATION.name)

    def durationSelectionFromName(name: String): DurationSelection = {
      val selection = name.toLowerCase match {
        case BY_MINIMUM_DURATION.name => Some(BY_MINIMUM_DURATION)
        case BY_MAXIMUM_DURATION.name => Some(BY_MAXIMUM_DURATION)
        case BY_AVERAGE_DURATION.name => Some(BY_AVERAGE_DURATION)
        case BY_TOTAL_DURATION.name => Some(BY_TOTAL_DURATION)
        case BY_COUNT_DURATION.name => Some(BY_COUNT_DURATION)
        case _ => None
      }
      require(selection.nonEmpty,
              s"invalid name $name specified, must be in ${supportedSelections()}")
      selection.get
    }

    /**
     * Select shortest session duration
     */
    final case object BY_MINIMUM_DURATION extends DurationSelection {
      val select: Column => Column = (col: Column) => min(col)
      val name: String = "minimum"
    }

    /**
     * Select longest session duration
     */
    final case object BY_MAXIMUM_DURATION extends DurationSelection {
      val select: Column => Column = (col: Column) => max(col)
      val name: String = "maximum"
    }

    /**
     * Select average session duration
     */
    final case object BY_AVERAGE_DURATION extends DurationSelection {
      val select: Column => Column = (col: Column) => avg(col)
      val name: String = "average"
    }

    /**
     * Select total session duration
     */
    final case object BY_TOTAL_DURATION extends DurationSelection {
      val select: Column => Column = (col: Column) => sum(col)
      val name: String = "total"
    }

    /**
     * Select count of session duration
     */
    final case object BY_COUNT_DURATION extends DurationSelection {
      val select: Column => Column = (col: Column) => count(col)
      val name: String = "count"
    }

  }

}
