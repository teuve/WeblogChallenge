package net.tev.ml

import net.tev.{BaseSparkTestSpec, LogParser}

class LoadPredictionSpec extends BaseSparkTestSpec {
  "LoadPrediction" should "generate proper data for training" in {
    val parser = new LogParser("src/test/resources/sample.log")
    val logs = parser.parse()
    val predictor = new LoadPrediction(logs)
    val trainingData = predictor.prepareTrainingData()
    assert(trainingData._2.length === 10)
    assert(!trainingData._2.contains("0-count"))
    assert(!trainingData._2.contains("0-users"))
    val fields = trainingData._1.schema.fieldNames
    for (idx <- 0 to 5) {
      assert(fields.contains(s"$idx-count"))
      assert(fields.contains(s"$idx-users"))
    }
  }

  it should "generate respect requested number of datapoints when generating data for training" in {
    val parser = new LogParser("src/test/resources/sample.log")
    val logs = parser.parse()
    val predictor = new LoadPrediction(logs, 10)
    val trainingData = predictor.prepareTrainingData()
    assert(trainingData._2.length === 20)
    assert(!trainingData._2.contains("0-count"))
    assert(!trainingData._2.contains("0-users"))
    val fields = trainingData._1.schema.fieldNames
    for (idx <- 0 to 10) {
      assert(fields.contains(s"$idx-count"))
      assert(fields.contains(s"$idx-users"))
    }
  }

  "LoadPrediction" should "generate proper data for predicting" in {
    val parser = new LogParser("src/test/resources/sample.log")
    val logs = parser.parse()
    val predictor = new LoadPrediction(logs)
    val trainingData = predictor.preparePredictionData()
    assert(trainingData._2.length === 10)
    assert(!trainingData._2.contains("0-count"))
    assert(!trainingData._2.contains("0-users"))
    val fields = trainingData._1.schema.fieldNames
    assert(!fields.contains("0-count"))
    assert(!fields.contains("0-users"))
    for (idx <- 1 to 5) {
      assert(fields.contains(s"$idx-count"))
      assert(fields.contains(s"$idx-users"))
    }
  }

  it should "generate respect requested number of datapoints when generating data for predicting" in {
    val parser = new LogParser("src/test/resources/sample.log")
    val logs = parser.parse()
    val predictor = new LoadPrediction(logs, 10)
    val trainingData = predictor.preparePredictionData()
    assert(trainingData._2.length === 20)
    assert(!trainingData._2.contains("0-count"))
    assert(!trainingData._2.contains("0-users"))
    val fields = trainingData._1.schema.fieldNames
    assert(!fields.contains("0-count"))
    assert(!fields.contains("0-users"))
    for (idx <- 1 to 10) {
      assert(fields.contains(s"$idx-count"))
      assert(fields.contains(s"$idx-users"))
    }
  }
}
