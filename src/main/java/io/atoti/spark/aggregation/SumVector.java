package io.atoti.spark.aggregation;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.col;

public record SumVector(String name, String column) implements AggregatedValue {
	
	public Column toAggregateColumn() {
		return col(column).alias(name);
	}

	public Column toColumn() {
		return col(name);
	}

	public String toSqlQuery() {
		throw new UnsupportedOperationException("TODO");
	}

}
