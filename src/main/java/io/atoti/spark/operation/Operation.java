package io.atoti.spark.operation;

import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;

import io.atoti.spark.aggregation.AggregatedValue;

public sealed abstract class Operation permits Multiply, Quantile, QuantileIndex, VectorAt {
	
	protected String name;
	protected Column column;
	protected List<AggregatedValue> neededAggregations;
	protected List<Operation> neededOperations;
	
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
		return Stream.concat(this.neededAggregations.stream(), this.neededOperations.stream().flatMap(Operation::getNeededAggregations));
	}
	
	public Stream<Operation> getAllOperations() {
		return Stream.concat(this.neededOperations.stream().flatMap(Operation::getAllOperations), Stream.of(this));
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final Operation op = (Operation) obj;
        return op.name.equals(this.name)
        		&& op.neededAggregations.equals(this.neededAggregations)
        		&& op.neededOperations.equals(this.neededOperations);
     }

	@Override
	public String toString() {
		return name + " | " + neededAggregations + " | " + neededOperations;
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
}
