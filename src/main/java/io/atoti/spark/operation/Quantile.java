package io.atoti.spark.operation;

import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.col;

import java.util.List;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import io.atoti.spark.Utils;
import io.atoti.spark.aggregation.AggregatedValue;
import scala.collection.immutable.ArraySeq;

public final class Quantile extends Operation {

	static UserDefinedFunction quantileUdf(float percent) {
		return udf((ArraySeq<Integer> arr) -> {
			var javaArr = Utils.convertScalaArrayToArray(arr);
			return Utils.quantile(javaArr, percent);
		}, 
		DataTypes.IntegerType);
	}

	
	public Quantile(String name, String arrayColumn, float percent) {
		this.name = name;
		this.column = quantileUdf(percent).apply(col(arrayColumn)).alias(name);
		this.neededColumns = List.of();
	}
	
	public Quantile(String name, AggregatedValue arrayColumn, float percent) {
		this.name = name;
		this.column = quantileUdf(percent).apply(arrayColumn.toColumn()).alias(name);
		this.neededColumns = List.of(arrayColumn);
	}

	public Quantile(String name, Operation arrayColumn, float percent) {
		this.name = name;
		this.column = quantileUdf(percent).apply(arrayColumn.toColumn()).alias(name);
		this.neededColumns = List.of(arrayColumn);
	}
}
