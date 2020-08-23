package com.clearpath.tweetpipesparkanalysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class TweetpipeSparkAnalyser {

    private static final String APPLICATION_NAME = "TweetPipe Spark analyser";
    private static final String STREAM_FORMAT = "kafka";
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "tracking";
    private static final String SPARK_MASTER = "local";

    private static final StructType TWEET_SCHEMA = new StructType()
            .add("text", StringType, false);

    public static void main(String[] args) throws StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession spark = SparkSession.builder()
                .appName(APPLICATION_NAME)
                .master(SPARK_MASTER)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> tweetDF = spark
                .readStream()
                .format(STREAM_FORMAT)
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", KAFKA_TOPIC)
                .load();

        Dataset<Row> tweetAndTimestampDF = tweetDF
                .select(col("timestamp"),
                        from_json(col("value").cast("string"), TWEET_SCHEMA)
                                .alias("tweet"))
                .alias("status")
                .select("status.*");

        Dataset<Row> window = tweetAndTimestampDF
                .withWatermark("timestamp", "1 minute")
                .groupBy(
                        window(col("timestamp"), "2 minutes", "1 minutes"),
                        col("tweet.text"))
                .count();

        window.printSchema();
        StreamingQuery query = window.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", true)
                .start();

        query.awaitTermination();
    }
}