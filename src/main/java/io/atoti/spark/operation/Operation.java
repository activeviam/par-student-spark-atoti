package io.atoti.spark.operation;

import static org.apache.spark.sql.functions.col;

import java.util.stream.Stream;

import org.apache.spark.sql.Column;

import io.atoti.spark.aggregation.AggregatedValue;

public sealed abstract class Operation permits Multiply, Quantile, QuantileIndex {
	
	protected String name;
	protected Column column;
	protected Stream<AggregatedValue> neededAggregations;
	protected Stream<Operation> neededOperations;
	
	public Column toAggregateColumn() {
		return this.column;
	}
	
	public Column toColumn() {
		return col(name);
	}
	public String getName() {
		return this.name;
	}
	
	public Stream<AggregatedValue> getNeededAggregations() {
		return Stream.concat(this.neededAggregations.flatMap((AggregatedValue agg) -> Stream.of(agg)), this.neededOperations.flatMap((Operation op) -> op.getNeededAggregations()));
	}
	
	public Stream<Operation> getAllOperations() {
		return Stream.concat(this.neededOperations.flatMap((Operation op) -> op.getAllOperations()), Stream.of(this));
	}
}
