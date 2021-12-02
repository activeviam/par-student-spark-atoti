package io.atoti.spark;

import io.atoti.spark.condition.QueryCondition;
import java.util.Arrays;
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
    final Column[] columns = wantedColumns.stream().map(functions::col).toArray(Column[]::new);
    if (limit >= 0) {
      return Arrays.asList((Row[]) dataframe.select(columns).limit(limit + offset).tail(limit));
    } else {
      return Arrays.asList(
          (Row[]) dataframe.select(columns).tail((int) dataframe.count() - offset));
    }
  }

  public static List<Row> list(Dataset<Row> dataframe, QueryCondition condition) {
    return dataframe.filter(condition.getCondition()).collectAsList();
  }

  public static List<Object> list(Object dataframe, QueryCondition condition) {
    throw new UnsupportedOperationException("TODO");
  }
}
