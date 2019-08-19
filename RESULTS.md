# WeblogChallenge

This files contains instructions for building and executing Spark code aiming at answering questions from [REAMDME](README.md) file.

## Bulding and packaging

Project has been built using Spark 2.4.3 and Scala 2.11. The packaging and configuration is managed by [SBT](https://www.scala-sbt.org/]. To package the application to be submitted to Spark, the following command can be used:

- sbt assembly

## Processing & Analytical

### Submitting to Spark

In order to run the application, I used the following command.

- $SPARK_HOME/bin/spark-submit --class net.tev.WeblogAnalysis --master local[4] target/scala-2.11/Weblog\ Challenge-assembly-1.0.jar -n TeVLogAnalysis -s data/2015_07_22_mktplace_shop_web_log_sample.log.gz -o target/sessions/ -d "minimum,maximum,average,total,count" -u "minimum,maximum,average,total,count"

The application takes a number of parameters:

- _-n_ or _-name_   For the application name to use
- _-s_ or _-source_ To specify where to read data from
- _-o_ or _-output_ To specify where to store session data (in parquet format)
- _-t_ or _-timeout_    To specify the session timeout in seconds, defaulting to 900 (15 minutes).
- _-d_ or _-durationStat_   To select which duration statistic(s) to compute, supported values are minimum, maximum, average, total and count. They can also be combined in CSV format.
- _-u_ or _-UserStat_   To select which duration statistic(s) to use when computing top users, supported values are minimum, maximum, average, total and count. They can also be combined in CSV format.
- _-n_ or _-numUsers_   To select the number of top users to list, which defaults to 10.

### Answers for processing and analytical questions

1. Sessionized data will be stored in output after running command.

2. Average session duration computed while running: 100.69901029402477 seconds

3. Unique URIs per session are part of session data saved in output when running command.

4. Most engaged users, depending on how we determine engagement (see full output below). Based on README description of longest sessions, "52.74.219.71", "119.81.61.166" and "106.186.23.95" are equal in first place with sessions of 2069 seconds.

### Full output from analysis run

```bash
19/08/17 14:36:06 INFO TeVLog: minimum duration: 0.0
19/08/17 14:36:06 INFO TeVLog: maximum duration: 2069.0
19/08/17 14:36:06 INFO TeVLog: average duration: 100.69901029402477
19/08/17 14:36:06 INFO TeVLog: total duration: 1.1161579E7
19/08/17 14:36:06 INFO TeVLog: count duration: 110841.0
19/08/17 14:36:06 INFO TeVLog: List top 10 users based on minimum session duration
19/08/17 14:36:06 INFO TeVLog: 125.16.218.194 has minimum session duration of 2065.0
19/08/17 14:36:06 INFO TeVLog: 103.29.159.138 has minimum session duration of 2065.0
19/08/17 14:36:06 INFO TeVLog: 14.99.226.79 has minimum session duration of 2063.0
19/08/17 14:36:06 INFO TeVLog: 122.169.141.4 has minimum session duration of 2060.0
19/08/17 14:36:06 INFO TeVLog: 14.139.220.98 has minimum session duration of 2058.0
19/08/17 14:36:06 INFO TeVLog: 117.205.158.11 has minimum session duration of 2057.0
19/08/17 14:36:06 INFO TeVLog: 111.93.89.14 has minimum session duration of 2055.0
19/08/17 14:36:06 INFO TeVLog: 182.71.63.42 has minimum session duration of 2051.0
19/08/17 14:36:06 INFO TeVLog: 223.176.3.130 has minimum session duration of 2048.0
19/08/17 14:36:06 INFO TeVLog: 183.82.103.131 has minimum session duration of 2042.0
19/08/17 14:36:06 INFO TeVLog: List top 10 users based on maximum session duration
19/08/17 14:36:06 INFO TeVLog: 52.74.219.71 has maximum session duration of 2069.0
19/08/17 14:36:06 INFO TeVLog: 119.81.61.166 has maximum session duration of 2069.0
19/08/17 14:36:06 INFO TeVLog: 106.186.23.95 has maximum session duration of 2069.0
19/08/17 14:36:06 INFO TeVLog: 125.19.44.66 has maximum session duration of 2068.0
19/08/17 14:36:06 INFO TeVLog: 125.20.39.66 has maximum session duration of 2068.0
19/08/17 14:36:06 INFO TeVLog: 180.211.69.209 has maximum session duration of 2067.0
19/08/17 14:36:06 INFO TeVLog: 54.251.151.39 has maximum session duration of 2067.0
19/08/17 14:36:06 INFO TeVLog: 192.8.190.10 has maximum session duration of 2067.0
19/08/17 14:36:06 INFO TeVLog: 180.179.213.70 has maximum session duration of 2066.0
19/08/17 14:36:06 INFO TeVLog: 203.189.176.14 has maximum session duration of 2066.0
19/08/17 14:36:06 INFO TeVLog: List top 10 users based on average session duration
19/08/17 14:36:06 INFO TeVLog: 103.29.159.138 has average session duration of 2065.0
19/08/17 14:36:06 INFO TeVLog: 125.16.218.194 has average session duration of 2065.0
19/08/17 14:36:06 INFO TeVLog: 14.99.226.79 has average session duration of 2063.0
19/08/17 14:36:06 INFO TeVLog: 122.169.141.4 has average session duration of 2060.0
19/08/17 14:36:06 INFO TeVLog: 14.139.220.98 has average session duration of 2058.0
19/08/17 14:36:06 INFO TeVLog: 117.205.158.11 has average session duration of 2057.0
19/08/17 14:36:06 INFO TeVLog: 111.93.89.14 has average session duration of 2055.0
19/08/17 14:36:06 INFO TeVLog: 182.71.63.42 has average session duration of 2051.0
19/08/17 14:36:06 INFO TeVLog: 223.176.3.130 has average session duration of 2048.0
19/08/17 14:36:06 INFO TeVLog: 183.82.103.131 has average session duration of 2042.0
19/08/17 14:36:06 INFO TeVLog: List top 10 users based on total session duration
19/08/17 14:36:06 INFO TeVLog: 220.226.206.7 has total session duration of 6795.0
19/08/17 14:36:06 INFO TeVLog: 52.74.219.71 has total session duration of 5258.0
19/08/17 14:36:06 INFO TeVLog: 119.81.61.166 has total session duration of 5253.0
19/08/17 14:36:06 INFO TeVLog: 54.251.151.39 has total session duration of 5236.0
19/08/17 14:36:06 INFO TeVLog: 121.58.175.128 has total session duration of 4988.0
19/08/17 14:36:06 INFO TeVLog: 106.186.23.95 has total session duration of 4931.0
19/08/17 14:36:06 INFO TeVLog: 125.19.44.66 has total session duration of 4653.0
19/08/17 14:36:06 INFO TeVLog: 54.169.191.85 has total session duration of 4621.0
19/08/17 14:36:06 INFO TeVLog: 207.46.13.22 has total session duration of 4535.0
19/08/17 14:36:06 INFO TeVLog: 180.179.213.94 has total session duration of 4512.0
19/08/17 14:36:06 INFO TeVLog: List top 10 users based on count session duration
19/08/17 14:36:06 INFO TeVLog: 220.226.206.7 has count session duration of 13.0
19/08/17 14:36:06 INFO TeVLog: 168.235.197.238 has count session duration of 10.0
19/08/17 14:36:06 INFO TeVLog: 54.241.32.108 has count session duration of 10.0
19/08/17 14:36:06 INFO TeVLog: 54.243.31.236 has count session duration of 10.0
19/08/17 14:36:06 INFO TeVLog: 54.251.31.140 has count session duration of 10.0
19/08/17 14:36:06 INFO TeVLog: 54.240.196.33 has count session duration of 10.0
19/08/17 14:36:06 INFO TeVLog: 207.46.13.22 has count session duration of 10.0
19/08/17 14:36:06 INFO TeVLog: 54.251.151.39 has count session duration of 10.0
19/08/17 14:36:06 INFO TeVLog: 185.20.4.220 has count session duration of 10.0
19/08/17 14:36:06 INFO TeVLog: 54.244.52.204 has count session duration of 10.0
```
## Machine Learning

### Predicting expected load

In order to predict load in the next minute, I used Spark's GeneralizedLinearRegression model and trained in on
counts of requests and unique users. For each minute, I've used the previous counts for the past 5 minutes as features.
In order to keep closer to what I expect production processes to be, I split the training and predicting in two phases
and stored/load the model in between. This allows training the model once and using it to predict data multiple times.
Production environments would normally train models at much slower rate than predictions, plus it allows to use the
newly trained model only if it performed well enough.

#### Running

In order to train the model, I used the following command.

- $SPARK_HOME/bin/spark-submit --class net.tev.ml.LoadPrediction --master local[4] target/scala-2.11/Weblog\ Challenge-assembly-1.0.jar -n TeVLogAnalysis -l data/2015_07_22_mktplace_shop_web_log_sample.log.gz -m target/model -o train

In order to predict using the model, I used the following command.

- $SPARK_HOME/bin/spark-submit --class net.tev.ml.LoadPrediction --master local[4] target/scala-2.11/Weblog\ Challenge-assembly-1.0.jar -n TeVLogAnalysis -l data/2015_07_22_mktplace_shop_web_log_sample.log.gz -m target/model -o predict

The application takes a number of parameters:

- _-n_ or _-name_   For the application name to use
- _-l_ or _-logs_ To specify where to read logs from
- _-m_ or _-model_ To specify where to store or load the model
- _-o_ or _-operation_    To specify whether to train the model or use it to predict (using a previously trained model)
- _-n_ or _-numDataPoints_   To select the number of datapoint (minutes to look back) to use for training and predicting (must match)

#### Results

Sample run produced results with low root mean square error (see below). Model saved under models/load-prediction.

```bash
19/08/18 17:31:28 INFO TeVLog: Generated model with rmse of 8.033577539856364E-12
```

Using this model to predict the amount of requests in the next minute would be around 2500.

```bash
19/08/18 17:33:32 INFO TeVLog: Predicting next minute will receive 2464.9999999999973 requests
```
