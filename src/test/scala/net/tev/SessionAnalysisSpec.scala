package net.tev

import org.scalatest.BeforeAndAfter

class SessionAnalysisSpec extends BaseSparkTestSpec with BeforeAndAfter {
  var analysis: SessionAnalysis = _

  before {
    import spark.implicits._
    val sessionEntries = for {
      idx <- 1 to 50
    } yield Session(s"1.2.3.${idx % 15}", idx, Seq("/path/1", "/path/2", "/path/1"), Set("/path/1", "/path/2"),
                    idx * 100 % 2000)

    val sessions = spark.sparkContext.parallelize(sessionEntries).toDS()
    analysis = new SessionAnalysis(sessions)
  }

  "SessionAnalysis" should "select top users from sample data" in {
    val logs = new LogParser("src/test/resources/sample.log").parse()
    val sessionizer = new Sessionizer(logs)
    val sessions = sessionizer.sessionize()
    val analysis = new SessionAnalysis(sessions)
    val topByAvg = analysis.topUsers()
    assert(topByAvg.size === 10)
    assert(topByAvg.head.clientIp === "66.220.146.22")
    assert(topByAvg.head.duration === 15.0)
    assert(topByAvg(9).clientIp === "203.90.70.251")
    assert(topByAvg(9).duration === 14.0)
  }

  it should "determine average duration from sample data" in {
    val logs = new LogParser("src/test/resources/sample.log").parse()
    val sessionizer = new Sessionizer(logs)
    val sessions = sessionizer.sessionize()
    val analysis = new SessionAnalysis(sessions)
    val mean = analysis.durationStats()
    assert(mean === 2.3921200750469045)
  }

  it should "select top users by minimum session duration" in {
    val topUser = analysis.topUsers(10, DurationSelection.BY_MINIMUM_DURATION)
    assert(topUser.size === 10)
    assert(topUser.head.clientIp === "1.2.3.0")
    assert(topUser.head.duration === 500.0)
    assert(topUser(9).clientIp === "1.2.3.12")
    assert(topUser(9).duration === 200.0)
  }

  it should "select top users by average session duration" in {
    val topUser = analysis.topUsers()
    assert(topUser.size === 10)
    assert(topUser.head.clientIp === "1.2.3.4")
    assert(topUser.head.duration === 1150.0)
    assert(topUser(9).clientIp === "1.2.3.13")
    assert(topUser(9).duration === 800.0)
  }

  it should "select top users by maximum session duration" in {
    val topUser = analysis.topUsers(10, DurationSelection.BY_MAXIMUM_DURATION)
    assert(topUser.size === 10)
    assert(topUser.head.clientIp === "1.2.3.4")
    assert(topUser.head.duration === 1900.0)
    assert(topUser(9).clientIp === "1.2.3.0")
    assert(topUser(9).duration === 1500.0)
  }

  it should "select top users by total session duration" in {
    val topUser = analysis.topUsers(10, DurationSelection.BY_TOTAL_DURATION)
    assert(topUser.size === 10)
    assert(topUser.head.clientIp === "1.2.3.4")
    assert(topUser.head.duration === 4600.0)
    assert(topUser(9).clientIp === "1.2.3.7")
    assert(topUser(9).duration === 2600.0)
  }

  it should "select top users by number of sessions" in {
    val topUser = analysis.topUsers(10, DurationSelection.BY_COUNT_DURATION)
    assert(topUser.size === 10)
    assert(topUser.head.clientIp === "1.2.3.3")
    assert(topUser.head.duration === 4.0)
    assert(topUser(9).clientIp === "1.2.3.10")
    assert(topUser(9).duration === 3.0)
  }

  it should "select only 5 top users" in {
    val topUser = analysis.topUsers(5)
    assert(topUser.size === 5)
    assert(topUser.head.clientIp === "1.2.3.4")
    assert(topUser.head.duration === 1150.0)
    assert(topUser(4).clientIp === "1.2.3.8")
    assert(topUser(4).duration === 966.6666666666666)
  }

  it should "determine minimum duration" in {
    val stat = analysis.durationStats(DurationSelection.BY_MINIMUM_DURATION)
    assert(stat === 0.0)
  }

  it should "determine average duration" in {
    val stat = analysis.durationStats()
    assert(stat === 870.0)
  }

  it should "determine maximum duration" in {
    val stat = analysis.durationStats(DurationSelection.BY_MAXIMUM_DURATION)
    assert(stat === 1900.0)
  }

  it should "determine total duration" in {
    val stat = analysis.durationStats(DurationSelection.BY_TOTAL_DURATION)
    assert(stat === 43500.0)
  }

  it should "determine number of sessions" in {
    val stat = analysis.durationStats(DurationSelection.BY_COUNT_DURATION)
    assert(stat === 50.0)
  }
}
