// SentimentAnalysisSetup.scala

// 1. Setting up the Spark session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("SentimentAnalysisApp").getOrCreate()

// 2. Loading and preprocessing the data
val data = spark.read.csv("path/to/reviews_dataset.csv")
val reviews = data.select("ReviewText")

// 3. Preparing the data for sentiment analysis
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, HashingTF, IDF}
val tokenizer = new Tokenizer().setInputCol("ReviewText").setOutputCol("Words")  // Tokenizing the review text into words
val remover = new StopWordsRemover().setInputCol("Words").setOutputCol("FilteredWords")  // Removing common stop words
val hashingTF = new HashingTF().setInputCol("FilteredWords").setOutputCol("RawFeatures")  // Transforming the words into feature vectors using hashing trick
val idf = new IDF().setInputCol("RawFeatures").setOutputCol("Features")  // Calculating the inverse document frequency for better weighting of the words

// 4. Configuring and training the logistic regression model
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
val lr = new LogisticRegression().setLabelCol("Label").setFeaturesCol("Features")  // Initializing the logistic regression model
val pipeline = new Pipeline().setStages(Array(tokenizer, remover, hashingTF, idf, lr))  // Setting up the pipeline for data processing and training
val model = pipeline.fit(data)  // Training the model on the data

// 5. Predicting sentiments and counting the results
val predictions = model.transform(data)  // Generating sentiment predictions for the data
val sentimentCounts = predictions.groupBy("Prediction").count()  // Counting the number of occurrences for each predicted sentiment
