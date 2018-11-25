package kioto.spark;

import kioto.Constants;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.Period;

public class SparkProcessor {

  private String brokers;

  public SparkProcessor(String brokers) {
    this.brokers = brokers;
  }

  public final void process() {

    SparkSession spark = SparkSession.builder()
        .appName("kioto")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> inputDataset = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", Constants.getHealthChecksTopic())
        .load();

    Dataset<Row> healthCheckJsonDf = inputDataset.selectExpr("CAST(value AS STRING)");

    StructType struct = new StructType()
        .add("event", DataTypes.StringType)
        .add("factory", DataTypes.StringType)
        .add("serialNumber", DataTypes.StringType)
        .add("type", DataTypes.StringType)
        .add("status", DataTypes.StringType)
        .add("lastStartedAt", DataTypes.StringType)
        .add("temperature", DataTypes.FloatType)
        .add("ipAddress", DataTypes.StringType);

    Dataset<Row> healthCheckNestedDs = healthCheckJsonDf.select(
        functions.from_json(new Column( "value" ), struct).as("healthCheck"));

    Dataset<Row> healthCheckFlattenedDs =
        healthCheckNestedDs.selectExpr("healthCheck.serialNumber", "healthCheck.lastStartedAt");

    Dataset<Row> healthCheckDs = healthCheckFlattenedDs
        .withColumn("lastStartedAt",
            functions.to_timestamp(new Column ("lastStartedAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));

    Dataset<Row> processedDs = healthCheckDs
        .withColumn( "lastStartedAt",
            new Column("uptime"));

    Dataset<Row> resDf = processedDs.select(
        functions.concat(new Column("serialNumber")).as("key"),
        processedDs.col("uptime").cast(DataTypes.StringType).as("value"));

    //StreamingQuery consoleOutput =
    processedDs.writeStream()
      .outputMode("append")
      .format("console")
      .start();

    //StreamingQuery kafkaOutput =
    resDf.writeStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", "uptimes")
        .start();

    try {
      spark.streams().awaitAnyTermination();
    } catch (StreamingQueryException e) {
      e.printStackTrace();
    }
  }

  private final int uptimeFunc(Timestamp date) {
    LocalDate localDate = date.toLocalDateTime().toLocalDate();
    return Period.between(localDate, LocalDate.now()).getDays();
  }

  public static void main(String[] args) {
    (new SparkProcessor("localhost:9092")).process();

  }

}
