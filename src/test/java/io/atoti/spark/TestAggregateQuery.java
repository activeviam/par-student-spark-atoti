package io.atoti.spark;

import io.atoti.spark.aggregation.Count;
import io.atoti.spark.aggregation.Max;
import io.atoti.spark.aggregation.Min;
import io.atoti.spark.aggregation.Sum;
import io.atoti.spark.condition.EqualCondition;
import io.atoti.spark.condition.TrueCondition;
import java.util.List;
import org.junit.jupiter.api.Test;

class TestAggregateQuery {

  @Test
  void testBasicAggregation() {
    final Object dataframe = null; // from basic.csv
    final var result =
        AggregateQuery.aggregate(
            dataframe,
            List.of("label"),
            List.of(new Count("c"), new Sum("s", "value"), new Max("M", "id")),
            TrueCondition.value());
    // result must have 2 values
    // for label = a -> c = 2, s = 25.91, M = 2
    // for label = b -> c = 1, s = -420, M = 3
  }

  @Test
  void testWithEmptyGroupBy() {
    final Object dataframe = null; // from basic.csv
    final var result =
        AggregateQuery.aggregate(
            dataframe,
            List.of(),
            List.of(new Count("c"), new Min("M", "id")),
            TrueCondition.value());
    // result must have 2 values
    // single row -> c = 3, m = 1
  }

  @Test
  void testListWithCondition() {
    final Object dataframe = null; // from basic.csv
    final var result =
        AggregateQuery.aggregate(
            dataframe, List.of("id"), List.of(new Count("c")), new EqualCondition("label", "a"));
    // result must have 2 values
    // id = 1 -> c = 1
    // id = 2 -> c = 1
  }
}
