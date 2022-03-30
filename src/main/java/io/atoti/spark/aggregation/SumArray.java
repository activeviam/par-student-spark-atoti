package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udaf;

import java.io.Serializable;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.UserDefinedFunction;

public final class SumArray implements AggregatedValue, Serializable {
  private static final long serialVersionUID = 8932076027241294986L;

  public String name;
  public String column;
  private UserDefinedFunction sumUdaf;

  private static long[] sum(long[] a, long[] b) {
    if (a.length == 0) {
      return b;
    }
    if (b.length == 0) {
      return a;
    }
    if (a.length != b.length) {
      throw new UnsupportedOperationException("Cannot sum arrays of different size");
    }
    return IntStream.range(0, a.length).mapToLong((int i) -> a[i] + b[i]).toArray();
  }

  public SumArray(String name, String column, Encoder<long[]> encoder) {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
    this.name = name;
    this.column = column;
    this.sumUdaf = udaf(
        new Aggregator<long[], long[], long[]>() {
          private static final long serialVersionUID = -6760989932234595260L;

          @Override
          public Encoder<long[]> bufferEncoder() {
            return encoder;
          }

          @Override
          public long[] finish(long[] reduction) {
            return reduction;
          }

          @Override
          public long[] merge(long[] b1, long[] b2) {
            return sum(b1, b2);
          }

          @Override
          public Encoder<long[]> outputEncoder() {
            return encoder;
          }

          @Override
          public long[] reduce(long[] b, long[] a) {
        	  return sum(a, b);
          }

          @Override
          public long[] zero() {
            return new long[0];
          }
        }, encoder);
  }

  public Column toAggregateColumn() {
    return sumUdaf.apply(col(this.column)).as(this.name);
  }

  public Column toColumn() {
    return col(this.name);
  }

  public String toSqlQuery() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (obj.getClass() != this.getClass()) {
      return false;
    }

    final SumArray sumArray = (SumArray) obj;
    return sumArray.name.equals(this.name) && sumArray.column.equals(this.column);
  }

  @Override
  public String toString() {
    return name + " | " + column;
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }
}
