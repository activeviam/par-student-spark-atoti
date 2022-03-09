/*
 * (C) ActiveViam 2022
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package io.atoti.spark;

import java.util.Comparator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import io.atoti.spark.aggregation.Sum;
import io.atoti.spark.aggregation.SumArray;
import io.atoti.spark.operation.Multiply;
import io.atoti.spark.operation.Quantile;
import io.atoti.spark.operation.QuantileIndex;
import io.atoti.spark.operation.VectorAt;

public class TestVectorAggregation {
	
  SparkSession spark =
		SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();
  
  @Test
  void quantile() {
	  final Dataset<Row> dataframe = CsvReader.read("csv/array.csv", spark);
	  var price_simulations = new SumArray("price_simulations_sum", "price_simulations", spark.implicits().newLongArrayEncoder());
	  var quantile = new Quantile("quantile", price_simulations, 95f);
	  var df = AggregateQuery.aggregate(dataframe, List.of("id", "price_simulations"), List.of(price_simulations), List.of(quantile));
	  df.show();
  }
  
  @Test
  void vectorAt() {
	  final Dataset<Row> dataframe = CsvReader.read("csv/array.csv", spark);
	  var price_simulations = new SumArray("price_simulations_bis", "price_simulations", spark.implicits().newLongArrayEncoder());
	  var vectorAt = new VectorAt("vector-at", price_simulations, 1);
	  var df = AggregateQuery.aggregate(dataframe, List.of("id", "price_simulations"), List.of(), List.of(vectorAt));
	  df.show();
  }
  
  @Test
  void simpleAggregation() {
    final Dataset<Row> dataframe = CsvReader.read("csv/array.csv", spark);
    var sumVector = new SumArray("sum(vector)", "price_simulations", spark.implicits().newLongArrayEncoder());
    System.out.println("LOGS");
    System.out.println(sumVector.toAggregateColumn());
    // dataframe.select(sumVector.toAggregateColumn());
    var df = AggregateQuery.aggregate(
            dataframe, List.of("id"), List.of(sumVector));
    System.out.println(df);
  }

  @Test
  void vectorScaling() {
    final Dataset<Row> dataframe = CsvReader.read("csv/array.csv", spark);
    final var f_vector = new Multiply(
        "f * vector",
        new Sum("f", "price"),
        new SumArray("sum(vector)", "price_simulations",  spark.implicits().newLongArrayEncoder())
    );
    AggregateQuery.aggregate(
            dataframe,
            List.of("id"),
            List.of(),
            List.of(f_vector))
        .collectAsList();
  }

  @Test
  void vectorQuantile() {
    final Dataset<Row> dataframe = null;
    AggregateQuery.aggregate(
            dataframe,
            List.of("simulation"),
            List.of(),
            List.of(
                new QuantileIndex(
                    "i95%",
                    new Multiply(
                        "f * vector",
                        new Sum("f", "factor-field"),
                        new Sum("sum(vector)", "vector-field")),
                    95f)))
        .collectAsList();
  }

  @Test
  void simulationExplorationAtQuantile() {
    final Dataset<Row> dataframe = null;
    final var revenues =
        new Multiply(
            "f * vector", new Sum("f", "factor-field"), new Sum("sum(vector)", "vector-field"));
    final List<Row> rows =
        AggregateQuery.aggregate(
                dataframe,
                List.of("simulation"),
                List.of(),
                List.of(
                    new Quantile("v95%", revenues, 95f), new QuantileIndex("i95%", revenues, 95f)))
            .collectAsList();
    final var compareByV95 = Comparator.<Row>comparingDouble(row -> row.getDouble(0));
    final int bestSimulation =
        rows.stream()
            .sorted(compareByV95.reversed())
            .mapToInt(row -> row.getInt(0))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No data to look at"));
    final int worstSimulation =
        rows.stream()
            .sorted(compareByV95)
            .mapToInt(row -> row.getInt(0))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No data to look at"));
    // Study the chosen simulations on the prices per category
    AggregateQuery.aggregate(
            dataframe,
            List.of("category"),
            List.of(),
            List.of(
                new VectorAt("revenue-at-best", revenues, bestSimulation),
                new VectorAt("revenue-at-worst", revenues, worstSimulation)))
        .collectAsList();
  }
}
