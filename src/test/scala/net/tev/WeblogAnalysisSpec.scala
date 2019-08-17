package net.tev

class WeblogAnalysisSpec extends BaseSparkTestSpec {
  "WeblogAnalysis" should "properly parse duration selection parameters" in {
    val selections = WeblogAnalysis.parseDurationSelection("minimum,maximum,average,total,count")
    assert(selections.size === 5)
    assert(DurationSelection.BY_MINIMUM_DURATION === selections.head)
    assert(DurationSelection.BY_MAXIMUM_DURATION === selections(1))
    assert(DurationSelection.BY_AVERAGE_DURATION === selections(2))
    assert(DurationSelection.BY_TOTAL_DURATION === selections(3))
    assert(DurationSelection.BY_COUNT_DURATION === selections(4))
  }

  it should "properly be able to parse single parameter" in {
    val selections = WeblogAnalysis.parseDurationSelection("minimum")
    assert(selections.size === 1)
    assert(DurationSelection.BY_MINIMUM_DURATION === selections.head)
  }

  it should "fail with proper message for invalid parameter" in {
    val error = intercept[IllegalArgumentException] {
      WeblogAnalysis.parseDurationSelection("minimum,maximum,avg,total,count")
    }
    assert(error.getMessage.contains("invalid name avg specified, must be in"))
  }
}
