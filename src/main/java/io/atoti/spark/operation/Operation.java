package io.atoti.spark.operation;

import java.util.stream.Stream;

import org.apache.spark.sql.Column;

import io.atoti.spark.aggregation.AggregatedValue;

public sealed interface Operation permits Multiply {
	public Column toAggregateColumn();
	public Column toColumn();
	public String getName();
	public Stream<AggregatedValue> getNeededAggregations();
	public Stream<Operation> getAllOperations();
}
