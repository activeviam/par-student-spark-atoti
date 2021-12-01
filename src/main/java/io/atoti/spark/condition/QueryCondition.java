package io.atoti.spark.condition;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public sealed interface QueryCondition permits
				AndCondition, OrCondition, NotCondition,
		EqualCondition, NullCondition,
	TrueCondition, FalseCondition {
		FilterFunction<Row> getCondition();
}
