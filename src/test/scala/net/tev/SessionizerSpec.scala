package net.tev

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfter

import scala.concurrent.duration._

class SessionizerSpec extends BaseSparkTestSpec with BeforeAndAfter {
  var sessionizer: Sessionizer = _

  before {
    import spark.implicits._

    val time = new AtomicLong(ZonedDateTime.parse("2015-07-22T09:00:00.000000Z").toEpochSecond * 1000)
    val logEntries = for {
      idx <- 1 to 50
    } yield Log(new Timestamp(time.addAndGet(idx * 1000)), "elb", IpPort("1.2.3.4", 987), IpPort("5.6.7.8", 321), 0.0,
                0.0, 0.0, 200, 200,
                3, 3, Request("GET", "https", "tev.net", 80, s"/some/path/$idx", "?q=test", "HTTP/1.1"), "agent",
                "cipher", "protocol")
    val logs = spark.sparkContext.parallelize(logEntries).toDS()
    sessionizer = new Sessionizer(logs)
  }


  "Sessionizer" should "properly group visits by client IP in sample data" in {
    import spark.implicits._
    // Use sample logs
    val logs = new LogParser("src/test/resources/sample.log").parse()
    val sessionizer = new Sessionizer(logs)
    val sessions = sessionizer.sessionize()
    assert(sessions.count === 1599)
    assert(sessions.first.isInstanceOf[Session])
    val validate = udf {
      (hits: Seq[String], uniques: Seq[String]) => {
        uniques.toSet != hits.toSet
      }
    }
    val invalid = sessions.withColumn("invalid", validate($"hits", $"uniques")).where($"invalid")
    if (!invalid.limit(1).isEmpty) {
      invalid.show(false)
      fail("Found session(s) with invalid or missing unique hits")
    }
  }

  it should "not group any hits" in {
    import spark.implicits._
    val sessions = sessionizer.sessionize(1.seconds.toSeconds)
    assert(sessions.count === 50)
    assert(sessions.first.isInstanceOf[Session])
    assert(sessions.where($"duration" > 1).limit(1).isEmpty)
  }

  it should "group all hits" in {
    val sessions = sessionizer.sessionize(60.seconds.toSeconds)
    assert(sessions.count === 1)
    assert(sessions.first.isInstanceOf[Session])
  }

  it should "group some hits" in {
    val sessions = sessionizer.sessionize(15.seconds.toSeconds)
    // First 14 hits should be joined, plus 36 remaining
    assert(sessions.count === 37)
    assert(sessions.first.isInstanceOf[Session])
  }
}
