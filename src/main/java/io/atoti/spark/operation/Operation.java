package io.atoti.spark.operation;

import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;

import io.atoti.spark.aggregation.AggregatedValue;

public sealed abstract class Operation permits Multiply, Quantile, QuantileIndex, VectorAt {
	
	protected String name;
	protected Column column;
	protected List<Object> neededColumns;
	
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
		return this.neededColumns.stream().flatMap((Object c) -> {
			if (c instanceof AggregatedValue) {
				  return List.of((AggregatedValue) c).stream();
			  } else if (c instanceof Operation) {
				  return ((Operation) c).getNeededAggregations();
			  } else {
				  throw new IllegalArgumentException("Unsupported type");
			  }
		});
	}
	
	public Stream<Operation> getAllOperations() {
		return Stream.concat(this.neededColumns.stream().flatMap((Object c) -> {
			if (c instanceof AggregatedValue) {
				  return Stream.empty();
			  } else if (c instanceof Operation) {
				  return ((Operation) c).getAllOperations();
			  } else {
				  throw new IllegalArgumentException("Unsupported type");
			  }
		}), List.of(this).stream());
	}
}
