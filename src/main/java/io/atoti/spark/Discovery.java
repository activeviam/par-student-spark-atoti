package io.atoti.spark;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

public class Discovery {

  private Discovery() {}

  private static Map<String, DataType> stringToDataTypesMap =
      Stream.of(
              new DataType[] {
                DataTypes.BinaryType,
                DataTypes.BooleanType,
                DataTypes.ByteType,
                DataTypes.CalendarIntervalType,
                DataTypes.DateType,
                DataTypes.DoubleType,
                DataTypes.FloatType,
                DataTypes.IntegerType,
                DataTypes.LongType,
                DataTypes.NullType,
                DataTypes.ShortType,
                DataTypes.StringType,
                DataTypes.TimestampType,
                DataTypes.createArrayType(DataTypes.IntegerType),
              })
          .collect(Collectors.toMap(DataType::toString, Function.identity()));

  public static Map<String, DataType> discoverDataframe(Dataset<Row> dataframe) {
    return Stream.of(dataframe.dtypes())
        .collect(
            Collectors.toMap(
                Tuple2<String, String>::_1,
                (Tuple2<String, String> t2) -> Discovery.stringToDataTypesMap.get(t2._2)));
  }
}
