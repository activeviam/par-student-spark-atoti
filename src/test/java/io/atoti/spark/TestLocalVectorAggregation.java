/*
 * (C) ActiveViam 2022
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.atoti.spark.aggregation.Sum;
import io.atoti.spark.aggregation.SumArray;
import io.atoti.spark.aggregation.SumArrayLength;
import io.atoti.spark.operation.Multiply;
import io.atoti.spark.operation.Quantile;
import io.atoti.spark.operation.QuantileIndex;
import io.atoti.spark.operation.VectorAt;
import io.github.cdimascio.dotenv.Dotenv;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.compat.immutable.ArraySeq;

public class TestVectorAggregation {

  static Dotenv dotenv = Dotenv.load();
  static SparkSession spark =
          SparkSession.builder()
                  .appName("Spark Atoti")
                  .config("spark.master", "local")
                  .config("spark.databricks.service.clusterId", dotenv.get("clusterId", "local-id"))
                  .getOrCreate();

  public TestVectorAggregation() {
    spark.sparkContext().setLogLevel("ERROR");
    spark.sparkContext().addJar("./target/spark-lib-0.0.1-SNAPSHOT.jar");
  }

  private static ArrayList<Long> convertScalaArrayToArray(ArraySeq<Long> arr) {
    return new ArrayList<Long>(JavaConverters.asJavaCollectionConverter(arr).asJavaCollection());
  }

  @SuppressWarnings("unchecked")
  @Test
  void quantile() {

    final Dataset<Row> dataframe = spark.read().table("array");
    var price_simulations =
        new SumArray(
            "price_simulations_sum", "price_simulations", spark.implicits().newLongArrayEncoder());
    var quantile = new Quantile("quantile", price_simulations, 95f);
    var rows =
        AggregateQuery.aggregate(
                dataframe, List.of("id"), List.of(price_simulations), List.of(quantile))
            .collectAsList();

    // result must have 2 values
    assertThat(rows).hasSize(2);

    final var rowsById =
        rows.stream().collect(Collectors.toUnmodifiableMap(row -> (row.getAs("id")), row -> (row)));

    assertThat((long) rowsById.get(1).getAs("quantile")).isEqualTo(7L);
    assertThat((long) rowsById.get(2).getAs("quantile")).isEqualTo(3L);
    for (int i = 0; i < 3; i++) {
      assertThat(
              convertScalaArrayToArray(
                      (ArraySeq<Long>) rowsById.get(1).getAs("price_simulations_sum"))
                  .get(i))
          .isEqualTo(List.of(3L, 7L, 5L).get(i));
      assertThat(
              convertScalaArrayToArray(
                      (ArraySeq<Long>) rowsById.get(2).getAs("price_simulations_sum"))
                  .get(i))
          .isEqualTo(List.of(1L, 3L, 2L).get(i));
    }
  }

  //
  @Test
  void vectorAt() {
    final Dataset<Row> dataframe = spark.read().table("array");
    var price_simulations =
        new SumArray(
            "price_simulations_bis", "price_simulations", spark.implicits().newLongArrayEncoder());
    var vectorAt = new VectorAt("vector-at", price_simulations, 1);
    var rows =
        AggregateQuery.aggregate(dataframe, List.of("id"), List.of(), List.of(vectorAt))
            .collectAsList();

    // result must have 2 values
    assertThat(rows).hasSize(2);

    final var rowsById =
        rows.stream().collect(Collectors.toUnmodifiableMap(row -> (row.getAs("id")), row -> (row)));

    assertThat((long) rowsById.get(1).getAs("vector-at")).isEqualTo(3);

    assertThat((long) rowsById.get(2).getAs("vector-at")).isEqualTo(1);
  }

  @SuppressWarnings("unchecked")
  @Test
  void simpleAggregation() {
    final Dataset<Row> dataframe = spark.read().table("array");
    var sumVector =
        new SumArray("sum(vector)", "price_simulations", spark.implicits().newLongArrayEncoder());
    var rows =
        AggregateQuery.aggregate(dataframe, List.of("id"), List.of(sumVector)).collectAsList();

    // result must have 2 values
    assertThat(rows).hasSize(2);

    final var rowsById =
        rows.stream().collect(Collectors.toUnmodifiableMap(row -> (row.getAs("id")), row -> (row)));

    for (int i = 0; i < 3; i++) {
      assertThat(
              convertScalaArrayToArray((ArraySeq<Long>) rowsById.get(1).getAs("sum(vector)"))
                  .get(i))
          .isEqualTo(List.of(3L, 7L, 5L).get(i));
      assertThat(
              convertScalaArrayToArray((ArraySeq<Long>) rowsById.get(2).getAs("sum(vector)"))
                  .get(i))
          .isEqualTo(List.of(1L, 3L, 2L).get(i));
    }
  }

  //
  @SuppressWarnings("unchecked")
  @Test
  void vectorScaling() {
    final Dataset<Row> dataframe = spark.read().table("array");
    final var f_vector =
        new Multiply(
            "f * vector",
            new Sum("f", "price"),
            new SumArray(
                "sum(vector)", "price_simulations", spark.implicits().newLongArrayEncoder()));
    var rows =
        AggregateQuery.aggregate(dataframe, List.of("id"), List.of(), List.of(f_vector))
            .collectAsList();

    // result must have 2 values
    assertThat(rows).hasSize(2);

    final var rowsById =
        rows.stream().collect(Collectors.toUnmodifiableMap(row -> (row.getAs("id")), row -> (row)));

    for (int i = 0; i < 3; i++) {
      assertThat(
              convertScalaArrayToArray((ArraySeq<Long>) rowsById.get(1).getAs("f * vector")).get(i))
          .isEqualTo(List.of(15L, 35L, 25L).get(i));
      assertThat(
              convertScalaArrayToArray((ArraySeq<Long>) rowsById.get(2).getAs("f * vector")).get(i))
          .isEqualTo(List.of(2L, 6L, 4L).get(i));
    }
  }

  //
  @Test
  void vectorQuantile() {
    final Dataset<Row> dataframe = spark.read().table("simulations");
    var rows =
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
                            new SumArray(
                                "sum(vector)",
                                "vector-field",
                                spark.implicits().newLongArrayEncoder())),
                        95f)))
            .collectAsList();

    // result must have 3 values
    assertThat(rows).hasSize(3);

    final var rowsById =
        rows.stream()
            .collect(Collectors.toUnmodifiableMap(row -> (row.getAs("simulation")), row -> (row)));

    assertThat((int) rowsById.get(1).getAs("i95%")).isEqualTo(7);

    assertThat((int) rowsById.get(2).getAs("i95%")).isEqualTo(7);

    assertThat((int) rowsById.get(3).getAs("i95%")).isEqualTo(7);
  }

  //
  @Test
  void simulationExplorationAtQuantile() {
    final Dataset<Row> dataframe = spark.read().table("simulations");
    final var revenues =
        new Multiply(
            "f * vector",
            new Sum("f", "factor-field"),
            new SumArray("sum(vector)", "vector-field", spark.implicits().newLongArrayEncoder()));
    final List<Row> rows =
        AggregateQuery.aggregate(
                dataframe,
                List.of("simulation"),
                List.of(),
                List.of(
                    new Quantile("v95%", revenues, 95f), new QuantileIndex("i95%", revenues, 95f)))
            .collectAsList();
    final var compareByV95 = Comparator.<Row>comparingLong(row -> (Long) (row.getAs("v95%")));
    final int bestSimulation =
        rows.stream()
            .sorted(compareByV95.reversed())
            .mapToInt(row -> (int) (row.getAs("i95%")))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No data to look at"));
    final int worstSimulation =
        rows.stream()
            .sorted(compareByV95)
            .mapToInt(row -> (int) (row.getAs("i95%")))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No data to look at"));
    // Study the chosen simulations on the prices per category
    var result =
        AggregateQuery.aggregate(
                dataframe,
                List.of("simulation"),
                List.of(),
                List.of(
                    new VectorAt("revenue-at-best", revenues, bestSimulation),
                    new VectorAt("revenue-at-worst", revenues, worstSimulation)))
            .collectAsList();

    // result must have 3 values
    assertThat(result).hasSize(3);

    final var rowsById =
        result.stream()
            .collect(Collectors.toUnmodifiableMap(row -> (row.getAs("simulation")), row -> (row)));

    assertThat((long) rowsById.get(1).getAs("revenue-at-best")).isEqualTo(840L);
    assertThat((long) rowsById.get(1).getAs("revenue-at-worst")).isEqualTo(840L);

    assertThat((long) rowsById.get(2).getAs("revenue-at-best")).isEqualTo(560L);
    assertThat((long) rowsById.get(2).getAs("revenue-at-worst")).isEqualTo(560L);

    assertThat((long) rowsById.get(3).getAs("revenue-at-best")).isEqualTo(690L);
    assertThat((long) rowsById.get(3).getAs("revenue-at-worst")).isEqualTo(690L);
  }

  @Test
  void testSumArrayLength() {
    final Dataset<Row> dataframe = spark.read().table("array");
    var price_simulations =
        new SumArrayLength(
            "price_simulations_sum", "price_simulations");
    var rows =
        AggregateQuery.aggregate(
                dataframe, List.of("id"), List.of(price_simulations), List.of())
            .collectAsList();

    // result must have 2 values
    System.out.println(rows);
  }
}
