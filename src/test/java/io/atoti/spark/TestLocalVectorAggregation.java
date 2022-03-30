/*
 * (C) ActiveViam 2022
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.atoti.spark.aggregation.SumArray;
import io.atoti.spark.aggregation.SumArrayLength;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class TestLocalVectorAggregation {

  SparkSession spark =
      SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

  public TestLocalVectorAggregation() {
    spark.sparkContext().setLogLevel("ERROR");
    spark.sparkContext().addJar("./target/spark-lib-0.0.1-SNAPSHOT.jar");
  }

  @Test
  void sumVector() {
    final Dataset<Row> dataframe = CsvReader.read("csv/array.csv", spark);
    var price_simulations =
        new SumArray(
            "price_simulations_bis", "price_simulations");
    var rows =
        AggregateQuery.aggregate(dataframe, List.of("id"), List.of(), List.of())
            .collectAsList();

    // result must have 2 values
    assertThat(rows).hasSize(2);

    final var rowsById =
        rows.stream().collect(Collectors.toUnmodifiableMap(row -> (row.getAs("id")), row -> (row)));

    assertThat((long) rowsById.get(1).getAs("vector-at")).isEqualTo(3);

    assertThat((long) rowsById.get(2).getAs("vector-at")).isEqualTo(1);
  }

  @Test
  void testSumArrayLength() {
    final Dataset<Row> dataframe = CsvReader.read("csv/array.csv", spark);
    var price_simulations =
        new SumArrayLength(
            "price_simulations_sum", "price_simulations");
    var rows =
        AggregateQuery.aggregate(
                dataframe, List.of(), List.of(price_simulations), List.of())
            .collectAsList();
    System.out.println(rows);
    assertThat(rows).hasSize(1)
        .extracting(row -> row.getLong(0))
        .containsExactlyInAnyOrder(6L);
  }
}
