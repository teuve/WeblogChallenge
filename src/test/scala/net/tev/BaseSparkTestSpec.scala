package net.tev

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

/**
 * Base Spec for Spark tests. Sets up the Spark session.
 */
class BaseSparkTestSpec extends FlatSpec with Matchers {
  implicit val spark: SparkSession = SparkSession.builder()
                                                 // Tests run locally
                                                 .master("local")
                                                 // Splitting in the default 200 partitions for shuffle is
                                                 // inefficient for testing locally on smallish data
                                                 .config("spark.sql.shuffle.partitions", "1")
                                                 .getOrCreate()
}
