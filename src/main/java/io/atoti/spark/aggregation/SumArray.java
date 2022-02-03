package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;
import scala.collection.mutable.ArraySeq;
import scala.jdk.CollectionConverters;

public final class SumArray implements AggregatedValue, Serializable {
  private static final long serialVersionUID = 8932076027241294986L;

  public String name;
  public String column;
  private Aggregator<Row, int[], int[]> udaf;

  private static int[] sum(int[] a, int[] b) {
    if (a.length == 0) {
      return b;
    }
    if (b.length == 0) {
      return a;
    }
    if (a.length != b.length) {
      throw new UnsupportedOperationException("Cannot sum arrays of different size");
    }
    return IntStream.range(0, a.length).map((int i) -> a[i] + b[i]).toArray();
  }

  public SumArray(String name, String column, Encoder<int[]> encoder) {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
    this.name = name;
    this.column = column;
    udaf =
        new Aggregator<Row, int[], int[]>() {
		  private static final long serialVersionUID = -6760989932234595260L;

		@Override
          public Encoder<int[]> bufferEncoder() {
            return encoder;
          }

          @Override
          public int[] finish(int[] reduction) {
            return reduction;
          }

          @Override
          public int[] merge(int[] b1, int[] b2) {
            return sum(b1, b2);
          }

          @Override
          public Encoder<int[]> outputEncoder() {
            return encoder;
          }

          @SuppressWarnings("unchecked")
          @Override
          public int[] reduce(int[] b, Row a) {
            ArraySeq<Integer> arraySeq;
            try {
              arraySeq = (ArraySeq<Integer>) a.getAs(column);
            } catch (ClassCastException e) {
              throw new UnsupportedOperationException("Column did not contains only arrays");
            }
            List<Integer> list = CollectionConverters.SeqHasAsJava(arraySeq).asJava();
            int[] array = list.stream().mapToInt(i -> i).toArray();
            return sum(array, b);
          }

          @Override
          public int[] zero() {
            return new int[0];
          }
        };
  }

  public Column toAggregateColumn() {
    return udaf.toColumn();
  }

  public Column toColumn() {
    return col(name);
  }

  public String toSqlQuery() {
    throw new UnsupportedOperationException("TODO");
  }
}
