package io.atoti.spark.aggregation;

import java.util.Objects;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import io.atoti.spark.Utils;

import scala.collection.immutable.ArraySeq;

import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.col;

public record Quantile(String name, AggregatedValue vectorValue, float percent) implements AggregatedValue {

	static UserDefinedFunction quantileUdf(float percent) {
		return udf((ArraySeq<Integer> arr) -> {
			var javaArr = Utils.convertScalaArrayToArray(arr);
			return Utils.quantile(javaArr, percent);
		}, 
		DataTypes.IntegerType);
	}

	public Quantile {
		Objects.requireNonNull(name, "No name provided");
		Objects.requireNonNull(vectorValue, "No vector value provided");
		if (percent < 0 || percent > 100) {
			throw new IllegalArgumentException("Percent must be 0 <= p <= 100");
		}
	}

	public Column toAggregateColumn() {
		return quantileUdf(percent).apply(vectorValue.toColumn()).alias(name);
	}

	public Column toColumn() {
		return col(name);
	}

	public String toSqlQuery() {
		throw new UnsupportedOperationException("TODO");
	}
}
