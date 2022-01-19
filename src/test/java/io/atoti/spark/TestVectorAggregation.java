/*
 * (C) ActiveViam 2022
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.atoti.spark.aggregation.Count;
import io.atoti.spark.aggregation.Multiply;
import io.atoti.spark.aggregation.Sum;
import io.atoti.spark.condition.EqualCondition;
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
}
