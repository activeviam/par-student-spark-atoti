package io.atoti.spark;

import io.atoti.spark.aggregation.SumArray;

import java.util.Arrays;
import java.util.List;

import io.atoti.spark.operation.Quantile;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

  static Dotenv dotenv = Dotenv.load();
  static SparkSession spark =
          SparkSession.builder()
                  .appName("Spark Atoti")
                  .config("spark.master", "local")
                  .config("spark.databricks.service.clusterId", dotenv.get("clusterId"))
                  .getOrCreate();

  public static void main(String[] args) {
    spark.sparkContext().addJar("./target/spark-lib-0.0.1-SNAPSHOT.jar");
    final Dataset<Row> dataframe = spark.read().table("array");
    SumArray price_simulations = new SumArray(
            "price_simulations_sum", "price_simulations"
    );
    Quantile quantile = new Quantile("quantile", price_simulations, 95f);
    List<Row> rows = AggregateQuery.aggregate(
                    dataframe, Arrays.asList("id"), Arrays.asList(price_simulations), Arrays.asList(quantile))
            .collectAsList();
    System.out.println(rows);
  }
}
