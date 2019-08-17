package net.tev

import java.sql.Timestamp
import java.time.ZonedDateTime

import org.apache.spark.SparkException

class LogParserSpec extends BaseSparkTestSpec {
  "LogParser" should "properly parse well formed logs" in {
    val parser = new LogParser("src/test/resources/sample.log")
    val logs = parser.parse()
    assert(logs.count() === 5000)
    assert(logs.first.isInstanceOf[Log])
  }

  it should "error out on malformed logs" in {
    val error = intercept[SparkException] {
      val parser = new LogParser("src/test/resources/bad_sample.log")
      val logs = parser.parse()
      // Force Spark to actually read the file
      logs.show
    }
    val cause = error.getCause
    assert(cause.isInstanceOf[IllegalArgumentException])
    assert(cause.getMessage.contains(
      "requirement failed: Log line could not be parsed: 2015-07-22T09:00:27.894580Z marketpalce-shop 203.91.211.4"))
  }

  it should "properly parse special case URIs and failed requests" in {
    val parser = new LogParser("src/test/resources/special_cases.log")
    val logs = parser.parse()
    // force to read
    logs.show(false)
  }

  it should "properly parse log lines" in {
    val line = "2015-07-22T16:10:50.917039Z marketpalce-shop 106.51.132.54:4170 10.0.4.150:80 0.000025 0.004734 " +
      "0.000014 200 200 0 13820 \"GET https://paytm.com:443/'\"()&%251<ScRiPt%20>prompt(981045)" +
      "</ScRiPt>/about?src='\"\\'\\\");|]*{%0d%0a<%00> HTTP/1.1\" \"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT " +
      "6.1; WOW64; Trident/5.0)\" DHE-RSA-AES128-SHA TLSv1"
    val log = LogParser.lineToLog(line)
    val time = new Timestamp(ZonedDateTime.parse("2015-07-22T16:10:50.917039Z").toEpochSecond * 1000)
    assert(log.time === time)
    assert(log.elb === "marketpalce-shop")
    assert(log.client.ip === "106.51.132.54")
    assert(log.client.port === 4170)
    assert(log.backend.ip === "10.0.4.150")
    assert(log.backend.port === 80)
    assert(log.requestProcessingTime === 0.000025)
    assert(log.backendProcessingTime === 0.004734)
    assert(log.responseProcessingTime === 0.000014)
    assert(log.elbStatusCode === 200)
    assert(log.backendStatusCode === 200)
    assert(log.receivedBytes === 0)
    assert(log.sentBytes === 13820)
    assert(log.request.method === "GET")
    assert(log.request.scheme === "https")
    assert(log.request.host === "paytm.com")
    assert(log.request.port === 443)
    assert(log.request.path === "/'\"()&%251<ScRiPt%20>prompt(981045)</ScRiPt>/about")
    assert(log.request.query === "src='\"\\'\\\");|]*{%0d%0a<%00>")
    assert(log.request.version === "HTTP/1.1")
    assert(log.userAgent === "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)")
    assert(log.sslCipher === "DHE-RSA-AES128-SHA")
    assert(log.sslProtocol === "TLSv1")
  }

  it should "properly parse line for failed request" in {
    val line = "2015-07-22T09:00:35.890581Z marketpalce-shop 106.66.99.116:37913 - -1 -1 -1 504 0 0 0 \"GET " +
      "https://paytm.com:443/shop/orderdetail/1116223940?channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT " +
      "6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.794.0 Safari/535.1\" ECDHE-RSA-AES128-SHA TLSv1"
    val log = LogParser.lineToLog(line)
    val time = new Timestamp(ZonedDateTime.parse("2015-07-22T09:00:35.890581Z").toEpochSecond * 1000)
    assert(log.time === time)
    assert(log.elb === "marketpalce-shop")
    assert(log.client.ip === "106.66.99.116")
    assert(log.client.port === 37913)
    assert(log.backend === null)
    assert(log.requestProcessingTime === -1)
    assert(log.backendProcessingTime === -1)
    assert(log.responseProcessingTime === -1)
    assert(log.elbStatusCode === 504)
    assert(log.backendStatusCode === 0)
    assert(log.receivedBytes === 0)
    assert(log.sentBytes === 0)
    assert(log.request.method === "GET")
    assert(log.request.scheme === "https")
    assert(log.request.host === "paytm.com")
    assert(log.request.port === 443)
    assert(log.request.path === "/shop/orderdetail/1116223940")
    assert(log.request.query === "channel=web&version=2")
    assert(log.request.version === "HTTP/1.1")
    assert(log
             .userAgent === "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.794.0 " +
      "Safari/535.1")
    assert(log.sslCipher === "ECDHE-RSA-AES128-SHA")
    assert(log.sslProtocol === "TLSv1")
  }
}
