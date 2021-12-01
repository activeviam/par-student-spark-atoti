package io.atoti.spark.condition;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public record NullCondition(String fieldName) implements QueryCondition {

	@Override
	public FilterFunction<Row> getCondition() {
		// TODO Auto-generated method stub
		return null;
	} }
