package net.tev.ml

import net.tev.{Log, LogParser}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
 * This predictor trains and applies models to predict amount of requests in the next minute based on past trend.
 *
 * @param logs          Path where access logs are located.
 * @param numDatapoints Number of datapoints ot look at. For each datapoint, the data from X minutes ago will be
 *                      added to the current row as a feature for the model to learn.
 * @param spark         The SparkSession in which to work.
 */
class LoadPrediction(logs: Dataset[Log], numDatapoints: Int = 5)(implicit spark: SparkSession) {

  /**
   * Train model using specified logs.
   *
   * @return Tuple containing trained model along with rsme (root squared mean error) score for it.
   */
  def train(): (CrossValidatorModel, Double) = {
    val preparedData = prepareTrainingData()
    val features = preparedData._2
    val data = preparedData._1

    val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features").setHandleInvalid("skip")
    val model = new GeneralizedLinearRegression().setFeaturesCol("features").setLabelCol("0-count")
                                                 .setTol(0.001)
    val stages = Array(assembler, model)
    val pipeline = new Pipeline().setStages(stages)
    val evaluator = new RegressionEvaluator().setLabelCol("0-count")

    val paramMap = new ParamGridBuilder()
      .addGrid(model.regParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
      .addGrid(model.family, Array("gaussian", "poisson", "gamma"))
      .build()
    val validator = new CrossValidator().setEvaluator(evaluator)
                                        .setEstimator(pipeline)
                                        .setEstimatorParamMaps(paramMap)
                                        .setParallelism(4)

    // Split data for training and validation
    val trainingAndTest = data.randomSplit(Array(0.8, 0.2))
    val training = trainingAndTest(0).cache
    // Train model using cross-validation
    val trained = validator.fit(training)
    // Evaluate model on test data
    val testResults = trained.transform(trainingAndTest(1))
    val score = new RegressionEvaluator().setLabelCol("0-count").setMetricName("rmse").evaluate(testResults)

    (trained, score)
  }

  /**
   * Predict number of requests in the next minute.
   *
   * @param model Model to use for predicting.
   * @return Predicted number of requests in the next minute.
   */
  def predictNextMinute(model: CrossValidatorModel): Double = {
    import spark.implicits._

    val preparedData = preparePredictionData()
    // Only need last minute of logs to predict the next.
    val data = preparedData._1.orderBy($"time".desc).limit(1)

    model.transform(data).first().getAs[Double]("prediction")
  }

  /**
   * Prepares the log data for training, according to the specified number of datapoints.
   *
   * @return Tuple containing Dataset with prepared data along with array of column names to use as features.
   */
  def prepareTrainingData(): (Dataset[Row], Array[String]) = {
    prepareData(0)
  }

  /**
   * Prepare data for predicting. Slight variation of training preparation as current data is stored in previous so that
   * model predicts next values.
   *
   * @return Tuple containing Dataset with prepared data along with array of column names to use for predicting.
   */
  def preparePredictionData(): (Dataset[Row], Array[String]) = {
    prepareData(1)
  }

  /**
   * Prepare dta for training or predicting, storing current data in offset specified by starting point.
   *
   * @param startingPoint Where to start storing current data (0 for training and 1 for predicting).
   * @return Tuple containing Dataset with prepared data along with array of column names to use.
   */
  def prepareData(startingPoint: Int): (Dataset[Row], Array[String]) = {
    import spark.implicits._

    val requestsPerMinute = logs.groupBy(window($"time", "1 minute").as("time"))
                                // Set current request count as previous value
                                .agg(count("time").as(s"$startingPoint-count"),
                                     // Also set current number of unique usres as previous value
                                     size(collect_set($"client.ip")).as(s"$startingPoint-users"))
    // Build up historical features
    val history = for {
      minutes <- startingPoint + 1 to numDatapoints
    } yield (
      s"$minutes-count",
      (count: Column) => lag(count, minutes - 1).over(Window.orderBy("time")),
      s"${minutes - 1}-count",
      s"$minutes-users",
      (users: Column) => lag(users, minutes - 1).over(Window.orderBy("time")),
      s"${minutes - 1}-users"
    )
    val trafficHistory = history.foldLeft(requestsPerMinute)(
      (df: Dataset[Row], hist: (String, Column => Column, String, String, Column => Column, String)) =>
        df.withColumn(hist._1, hist._2(col(s"$startingPoint-count")))
          // Assume constant traffic for first minutes
          .withColumn(hist._1, when(col(hist._1).isNull, col(hist._3)).otherwise(col(hist._1)))
          .withColumn(hist._4, hist._5(col(s"$startingPoint-users")))
          // Assume constant number of users for first minutes
          .withColumn(hist._4, when(col(hist._4).isNull, col(hist._6)).otherwise(col(hist._4)))
      )

    // Build features, always starts at 1
    val featurePairs = for {
      idx <- 1 to numDatapoints
    } yield (s"$idx-count", s"$idx-users")
    val features = featurePairs.flatMap(pair => Seq(pair._1, pair._2)).toArray

    (trafficHistory, features)
  }
}

object LoadPrediction {
  val LOGGER: Logger = LoggerFactory.getLogger("TeVLog")

  def main(args: Array[String]): Unit = {
    val parsedConfig = parser.parse(args, Config())
    require(parsedConfig.nonEmpty, "Invalid parameters")

    implicit val spark: SparkSession = SparkSession.builder().appName(parsedConfig.get.name).getOrCreate()

    LOGGER.info(s"Loading logs from ${parsedConfig.get.logs}")
    val logParser = new LogParser(parsedConfig.get.logs)
    val logs = logParser.parse()
    LOGGER.info(s"Using ${parsedConfig.get.numDatapoints} datapoints in the past")
    val predictor = new LoadPrediction(logs, parsedConfig.get.numDatapoints)

    val message = parsedConfig.get.operation match {
      case "train" =>
        val trainingResult = predictor.train()
        trainingResult._1.save(parsedConfig.get.model)
        Some(s"Generated model with rmse of ${trainingResult._2}")

      case "predict" =>
        val model = CrossValidatorModel.load(parsedConfig.get.model)
        val predictionResult = predictor.predictNextMinute(model)
        Some(s"Predicting next minute will receive $predictionResult requests")

      case _ => None
    }
    require(message.nonEmpty, s"Operation ${parsedConfig.get.operation} is unsupported, must be train or predict")
    LOGGER.info(message.get)
  }

  val parser: OptionParser[Config] = new OptionParser[Config]("TeVSpark") {
    opt[String]('n', "name").required().action((n, conf) => conf.copy(name = n)).text("name of the Spark application")
    opt[String]('m', "model").required().action((m, conf) => conf.copy(model = m)).text("Path where to save/load model")
    opt[String]('l', "logs").required().action((l, conf) => conf.copy(logs = l))
                            .text("Path to logs used to train/predict")
    opt[Int]('n', "numDataPoints").action((n, conf) => conf.copy(numDatapoints = n)).text(
      "Number of datapoints to use for training/predicting. Number used for prediction must match number used in " +
        "training. Defaults to 5.")
    opt[String]('o', "operation").action((o, conf) => conf.copy(operation = o))
                                 .text("Operation to perform, either train or predict.")
  }

  final case class Config(name: String = "",
                          model: String = "",
                          logs: String = "",
                          operation: String = "predict",
                          numDatapoints: Int = 5)

}
