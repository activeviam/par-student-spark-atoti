package io.atoti.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Discovery {

	private Discovery() {}

	public static Map<String, String> discoverDataframe(Dataset<Row> dataframe) {
		return Stream.of(dataframe.dtypes())
				.collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
	}
}
