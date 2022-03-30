package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;
import scala.collection.compat.immutable.ArraySeq;
import scala.collection.mutable.WrappedArray;

public final class SumArrayLength implements AggregatedValue, Serializable {
  private static final long serialVersionUID = 20220330_0933L;

  public String name;
  public String column;
  private Aggregator<Row, Long, Long> udaf;

  public SumArrayLength(final String name, final String column) {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
    this.name = name;
    this.column = column;
    this.udaf =
        new Aggregator<Row, Long, Long>() {
          private static final long serialVersionUID = 20220330_1005L;

          @Override
          public Encoder<Long> bufferEncoder() {
            return Encoders.LONG();
          }

          @Override
          public Long finish(final Long reduction) {
            return reduction;
          }

          @Override
          public Long merge(final Long a, final Long b) {
            return a + b;
          }

          @Override
          public Encoder<Long> outputEncoder() {
            return Encoders.LONG();
          }

          @Override
          public Long reduce(final Long result, final Row row) {
            final WrappedArray<Long> arraySeq;
            try {
              arraySeq = row.getAs(column);
            } catch (final ClassCastException e) {
              throw new UnsupportedOperationException("Column did not contains only arrays", e);
            }
            return result + arraySeq.length();
          }

          @Override
          public Long zero() {
            return 0L;
          }
        };
  }

  public Column toAggregateColumn() {
    return this.udaf.toColumn().as(this.name);
  }

  public Column toColumn() {
    return col(this.name);
  }

  public String toSqlQuery() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }

    if (obj.getClass() != this.getClass()) {
      return false;
    }

    final SumArrayLength sumArray = (SumArrayLength) obj;
    return sumArray.name.equals(this.name) && sumArray.column.equals(this.column);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + this.name + " | " + this.column + "]";
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }
}
