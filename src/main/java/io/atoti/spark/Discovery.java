package io.atoti.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Discovery {

	private Discovery() {}

	public static void discoverDataframe(Dataset<Row> dataframe) {
		dataframe.printSchema();
		dataframe.show();
	}
}
