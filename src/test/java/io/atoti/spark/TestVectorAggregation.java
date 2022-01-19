/*
 * (C) ActiveViam 2022
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package io.atoti.spark;

import io.atoti.spark.aggregation.Multiply;
import io.atoti.spark.aggregation.Quantile;
import io.atoti.spark.aggregation.QuantileIndex;
import io.atoti.spark.aggregation.Sum;
import io.atoti.spark.aggregation.VectorAt;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class TestVectorAggregation {

  @Test
  void simpleAggregation() {
    final Dataset<Row> dataframe = null;
    AggregateQuery.aggregate(
            dataframe, List.of("simulation"), List.of(new Sum("sum(vector)", "vector-field")))
        .collectAsList();
  }

  @Test
  void vectorScaling() {
    final Dataset<Row> dataframe = null;
    AggregateQuery.aggregate(
            dataframe,
            List.of("simulation"),
            List.of(
                new Multiply(
                    "f * vector",
                    new Sum("f", "factor-field"),
                    new Sum("sum(vector)", "vector-field"))))
        .collectAsList();
  }

  @Test
  void vectorQuantile() {
    final Dataset<Row> dataframe = null;
    AggregateQuery.aggregate(
            dataframe,
            List.of("simulation"),
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
    AggregateQuery.aggregate(
            dataframe,
            List.of("simulation"),
            List.of(
                new VectorAt("revenue-at-best", revenues, bestSimulation),
                new VectorAt("revenue-at-worst", revenues, worstSimulation)))
        .collectAsList();
  }
}
