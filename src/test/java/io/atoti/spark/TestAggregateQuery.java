package io.atoti.spark;

import io.atoti.spark.aggregation.Avg;
import io.atoti.spark.aggregation.Count;
import io.atoti.spark.aggregation.Max;
import io.atoti.spark.aggregation.Min;
import io.atoti.spark.aggregation.Sum;
import io.atoti.spark.condition.EqualCondition;
import io.atoti.spark.condition.TrueCondition;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class TestAggregateQuery {

	SparkSession spark = SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

	@Test
	void testBasicAggregation() {
		final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
		final var rows = AggregateQuery
				.aggregate(dataframe, List.of("label"),
						List.of(
							new Count("c"),
							new Sum("s", "value"),
							new Max("M", "id"),
							new Avg("avg", "value")))
				.collectAsList();

		// result must have 2 values
		assertThat(rows).hasSize(2);

		final var rowsByLabel = rows.stream()
				.collect(Collectors.toUnmodifiableMap(row -> (row.getAs("label")), row -> (row)));

		// for label = a -> c = 2, s = 25.91, M = 2
		final var rowA = rowsByLabel.get("a");
		assertThat((long) rowA.getAs("c")).isEqualTo(2);
		assertThat((double) rowA.getAs("s")).isEqualTo(25.91);
		assertThat((int) rowA.getAs("M")).isEqualTo(2);
		assertThat((double) rowA.getAs("avg")).isEqualTo(12.955);

		// for label = b -> c = 1, s = -420, M = 3
		final var rowB = rowsByLabel.get("b");
		assertThat((long) rowB.getAs("c")).isEqualTo(1);
		assertThat((double) rowB.getAs("s")).isEqualTo(-420);
		assertThat((int) rowB.getAs("M")).isEqualTo(3);
	}

	@Test
	void testWithEmptyGroupBy() {
		final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
		final var rows = AggregateQuery
				.aggregate(dataframe, List.of(), List.of(new Count("c"), new Min("M", "id")))
				.collectAsList();

		// result must have only 1 value
		assertThat(rows).hasSize(1);

		// single row -> c = 3, M = 1
		assertThat((long) rows.get(0).getAs("c")).isEqualTo(3);
		assertThat((int) rows.get(0).getAs("M")).isEqualTo(1);
	}

	@Test
	void testListWithCondition() {
		final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
		final var rows = AggregateQuery.aggregate(dataframe, List.of("id"), List.of(new Count("c")),
				new EqualCondition("label", "a")).collectAsList();
		
		// result must have 2 values
		assertThat(rows).hasSize(2);
		
		final var rowsById = rows.stream()
				.collect(Collectors.toUnmodifiableMap(row -> (row.getAs("id")), row -> (row)));

		// id = 1 -> c = 1
		final var row1 = rowsById.get(1);
		assertThat((long) row1.getAs("c")).isEqualTo(1);
		
		// id = 2 -> c = 1
		final var row2 = rowsById.get(2);
		assertThat((long) row2.getAs("c")).isEqualTo(1);
	}
}
