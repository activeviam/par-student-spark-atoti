package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

public class TestDiscovery {

  static Dotenv dotenv = Dotenv.load();
  static SparkSession spark =
      SparkSession.builder()
              .appName("Spark Atoti")
              .config("spark.master", "local")
              .config("spark.databricks.service.clusterId", dotenv.get("clusterId"))
              .getOrCreate();

  @Test
  void testDiscoveryBasis() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final Map<String, DataType> dTypes = Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType);
    assertTrue(dTypes.containsKey("label"));
    assertThat(dTypes.get("label")).isEqualTo(DataTypes.StringType);
    assertTrue(dTypes.containsKey("value"));
    assertThat(dTypes.get("value")).isEqualTo(DataTypes.DoubleType);
    assertTrue(dTypes.containsKey("date"));
    assertThat(dTypes.get("date")).isEqualTo(DataTypes.TimestampType);
  }

  @Test
  void testDiscoveryTwoTypesInSameColumn() {
    final Dataset<Row> dataframe = spark.read().table("twotypesinsamecolumn");
    final Map<String, DataType> dTypes = Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType);
    assertTrue(dTypes.containsKey("label"));
    assertThat(dTypes.get("label")).isEqualTo(DataTypes.StringType);
    assertTrue(dTypes.containsKey("value"));
    assertThat(dTypes.get("value")).isEqualTo(DataTypes.StringType);
    assertTrue(dTypes.containsKey("date"));
    assertThat(dTypes.get("date")).isEqualTo(DataTypes.StringType);
  }

  @Test
  void testDiscoveryJoin() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final Dataset<Row> dataframeToJoin = spark.read().table("tojoin");

    final Dataset<Row> dataframeJoin =
        dataframe.join(
            dataframeToJoin, dataframeToJoin.col("basic_id").equalTo(dataframe.col("id")), "inner");
    final Map<String, DataType> dTypes = Discovery.discoverDataframe(dataframeJoin);

    assertThat(dataframe).isNotNull();
    assertThat(dataframeJoin).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType);
    assertTrue(dTypes.containsKey("label"));
    assertThat(dTypes.get("label")).isEqualTo(DataTypes.StringType);
    assertTrue(dTypes.containsKey("value"));
    assertThat(dTypes.get("value")).isEqualTo(DataTypes.DoubleType);
    assertTrue(dTypes.containsKey("date"));
    assertThat(dTypes.get("date")).isEqualTo(DataTypes.TimestampType);
    assertTrue(dTypes.containsKey("join_id"));
    assertThat(dTypes.get("join_id")).isEqualTo(DataTypes.IntegerType);
    assertTrue(dTypes.containsKey("join_value"));
    assertThat(dTypes.get("join_value")).isEqualTo(DataTypes.DoubleType);
    assertTrue(dTypes.containsKey("join_date"));
    assertThat(dTypes.get("join_date")).isEqualTo(DataTypes.TimestampType);
  }

  @Test
  void testDiscoveryCalculate() {
    Dataset<Row> dataframe = spark.read().table("calculate");
    dataframe =
        dataframe
            .withColumn("val1_minus_val2", dataframe.col("val1").minus(dataframe.col("val2")))
            .withColumn("val1_equal_val2", dataframe.col("val1").equalTo(dataframe.col("val2")));
    final Map<String, DataType> dTypes = Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType);
    assertTrue(dTypes.containsKey("val1"));
    assertThat(dTypes.get("val1")).isEqualTo(DataTypes.IntegerType);
    assertTrue(dTypes.containsKey("val2"));
    assertThat(dTypes.get("val2")).isEqualTo(DataTypes.DoubleType);
    assertTrue(dTypes.containsKey("val1_minus_val2"));
    assertThat(dTypes.get("val1_minus_val2")).isEqualTo(DataTypes.DoubleType);
    assertTrue(dTypes.containsKey("val1_equal_val2"));
    assertThat(dTypes.get("val1_equal_val2")).isEqualTo(DataTypes.BooleanType);
  }

  @Test
  void testDiscoveryArray() {
    Dataset<Row> dataframe = spark.read().table("array");
    final Map<String, DataType> dTypes = Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dTypes).isNotNull();
    assertThat(dTypes.get("price_simulations"))
        .isEqualTo(DataTypes.createArrayType(DataTypes.LongType));
  }
}
