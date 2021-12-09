package io.atoti.spark;

import static org.apache.spark.sql.functions.monotonically_increasing_id;

import io.atoti.spark.condition.QueryCondition;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ListQuery {

  /**
   * Retrieves a series of rows from a dataframe.
   *
   * @param wantedColumns names of the columns we want to see in the resulting list
   * @param limit max number of rows to return. Passing a negative number disable this option
   * @param offset offset at which this starts reading the input dataframe
   * @return list of rows extracted from the DataFrame
   */
  public static List<Row> list(
      Dataset<Row> dataframe, List<String> wantedColumns, int limit, int offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("Cannot accept a negative offset");
    }

    final Column[] columns = wantedColumns.stream().map(functions::col).toArray(Column[]::new);
    dataframe = dataframe.withColumn("_id", monotonically_increasing_id());

    if (limit < 0) {
      return dataframe.where(dataframe.col("_id").geq(offset)).select(columns).collectAsList();
    } else {
      return dataframe
          .where(dataframe.col("_id").between(offset, offset + limit - 1))
          .select(columns)
          .collectAsList();
    }
  }

  public static List<Row> list(Dataset<Row> dataframe, QueryCondition condition) {
    return dataframe.filter(condition.getCondition()).collectAsList();
  }
}
