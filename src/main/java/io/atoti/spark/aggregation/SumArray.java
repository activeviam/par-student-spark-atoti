package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.col;

import io.atoti.spark.Utils;
import java.io.Serializable;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import scala.collection.IndexedSeq;

public final class SumArray implements AggregatedValue, Serializable {
  private static final long serialVersionUID = 8932076027241294986L;

  public String name;
  public String column;
  private Aggregator<?, ?, ?> udaf;

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

  private static long[] sum(long[] buffer, IndexedSeq<Long> value) {
    if (buffer.length == 0) {
      return Utils.convertScalaArrayToArray(value).stream().mapToLong(Long::longValue).toArray();
    }
    if (value.length() == 0) {
      return buffer;
    }
    if (buffer.length != value.length()) {
      throw new UnsupportedOperationException("Cannot sum arrays of different size");
    }
    return IntStream.range(0, buffer.length).mapToLong((int i) -> buffer[i] + value.apply$mcJI$sp(i)).toArray();
  }

  public SumArray(String name, String column) {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
    this.name = name;
    this.column = column;
    this.udaf =
        new Aggregator<Row, long[], long[]>() {
          private static final long serialVersionUID = -6760989932234595260L;

          @Override
          public Encoder<long[]> bufferEncoder() {
            return SparkSession.active().implicits().newLongArrayEncoder();
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
            return SparkSession.active().implicits().newLongArrayEncoder();
          }

          @SuppressWarnings("unchecked")
          @Override
          public long[] reduce(long[] buffer, Row row) {
            IndexedSeq<Long> arraySeq;
            try {
              arraySeq = row.getAs(column);
            } catch (ClassCastException e) {
              throw new UnsupportedOperationException("Column did not contains only arrays", e);
            }
            final long[] result = sum(buffer, arraySeq);
            return result;
          }

          @Override
          public long[] zero() {
            return new long[0];
          }
        };
  }

  public Column toAggregateColumn() {
    return udaf.toColumn().as(this.name);
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
