package io.atoti.spark.operation;

import static io.atoti.spark.Utils.convertScalaArrayToArray;
import static io.atoti.spark.Utils.convertToArrayListToScalaArraySeq;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import io.atoti.spark.aggregation.AggregatedValue;
import scala.collection.immutable.ArraySeq;

public final class Multiply implements Operation {
			
	private static UserDefinedFunction udf = udf((Long x, ArraySeq<Integer> s) -> {
		ArrayList<Integer> list = convertScalaArrayToArray(s);
		return convertToArrayListToScalaArraySeq(list.stream().map((Integer value) -> value * x).toList());
	}, DataTypes.createArrayType(DataTypes.LongType));
	
	private String name;
	private Column column;
	private List<Object> neededColumns;

	  public Multiply(String name, String scalarColumn, String arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(col(scalarColumn), col(arrayColumn)).alias(name);
		  this.neededColumns = List.of();
	  }

	  public Multiply(String name, String scalarColumn, AggregatedValue arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(col(scalarColumn), arrayColumn.toColumn()).alias(name);
		  this.neededColumns = List.of(arrayColumn);
	  }

	  public Multiply(String name, String scalarColumn, Operation arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(col(scalarColumn), arrayColumn.toColumn()).alias(name);
		  this.neededColumns = List.of(arrayColumn);
	  }

	  public Multiply(String name, AggregatedValue scalarColumn, String arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(scalarColumn.toColumn(), col(arrayColumn)).alias(name);
		  this.neededColumns = List.of(scalarColumn);
	  }

	  public Multiply(String name, AggregatedValue scalarColumn, AggregatedValue arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(scalarColumn.toColumn(), arrayColumn.toColumn()).alias(name);
		  this.neededColumns = List.of(scalarColumn, arrayColumn);
	  }

	  public Multiply(String name, AggregatedValue scalarColumn, Operation arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(scalarColumn.toColumn(), arrayColumn.toColumn()).alias(name);
		  this.neededColumns = List.of(scalarColumn, arrayColumn);
	  }

	  public Multiply(String name, Operation scalarColumn, String arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(scalarColumn.toColumn(), col(arrayColumn)).alias(name);
		  this.neededColumns = List.of(scalarColumn);
	  }

	  public Multiply(String name, Operation scalarColumn, AggregatedValue arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(scalarColumn.toColumn(), arrayColumn.toColumn()).alias(name);
		  this.neededColumns = List.of(scalarColumn, arrayColumn);
	  }

	  public Multiply(String name, Operation scalarColumn, Operation arrayColumn) {
		  this.name = name;
		  this.column = Multiply.udf.apply(scalarColumn.toColumn(), arrayColumn.toColumn()).alias(name);
		  this.neededColumns = List.of(scalarColumn, arrayColumn);
	  }

  public Column toAggregateColumn() {
	  return this.column;
  }
  @Override
  public Column toColumn() {
    return col(this.name);
  }
  
  @Override
  public String getName() {
	  return this.name;
  }

  public String toSqlQuery() {
    throw new UnsupportedOperationException("TODO");
  }

	@Override
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

	@Override
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
