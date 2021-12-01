package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;


public class TestDiscovery {
  SparkSession spark =
      SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

  @Test
  void testDiscoveryBasis() throws URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final URL url = Objects.requireNonNull(cl.getResource("csv/basic.csv"), "Cannot find file");

    final Dataset<Row> dataframe = spark.read()
            .format("csv")
            .option("sep", ";")
            .option("header", true)
            .option("timestampFormat", "dd/MM/yyyy")
            .option("inferSchema", true)
            .load(url.toURI().getPath());
    final Map<String, String> dTypes = Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType.toString());
    assertTrue(dTypes.containsKey("label"));
    assertThat(dTypes.get("label")).isEqualTo(DataTypes.StringType.toString());
    assertTrue(dTypes.containsKey("value"));
    assertThat(dTypes.get("value")).isEqualTo(DataTypes.DoubleType.toString());
    assertTrue(dTypes.containsKey("date"));
    assertThat(dTypes.get("date")).isEqualTo(DataTypes.TimestampType.toString());
  }

  @Test
  void testDiscoveryTwoTypesInSameColumn() throws URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final URL url =
            Objects.requireNonNull(cl.getResource("csv/twoTypesInSameColumn.csv"), "Cannot find file");

    final Dataset<Row> dataframe = spark.read()
            .format("csv")
            .option("sep", ";")
            .option("header", true)
            .option("timestampFormat", "dd/MM/yyyy")
            .option("inferSchema", true)
            .load(url.toURI().getPath());
    final Map<String, String> dTypes = Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType.toString());
    assertTrue(dTypes.containsKey("label"));
    assertThat(dTypes.get("label")).isEqualTo(DataTypes.StringType.toString());
    assertTrue(dTypes.containsKey("value"));
    assertThat(dTypes.get("value")).isEqualTo(DataTypes.StringType.toString());
    assertTrue(dTypes.containsKey("date"));
    assertThat(dTypes.get("date")).isEqualTo(DataTypes.StringType.toString());
  }

  @Test
  void testDiscoveryJoin() throws URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final URL url = Objects.requireNonNull(cl.getResource("csv/basic.csv"), "Cannot find file");
    final URL urlToJoin = Objects.requireNonNull(cl.getResource("csv/toJoin.csv"), "Cannot find file");

    final Dataset<Row> dataframe = spark.read()
            .format("csv")
            .option("sep", ";")
            .option("header", true)
            .option("timestampFormat", "dd/MM/yyyy")
            .option("inferSchema", true)
            .load(url.toURI().getPath());
    final Dataset<Row> dataframeToJoin = spark.read()
          .format("csv")
          .option("sep", ";")
          .option("header", true)
          .option("timestampFormat", "dd/MM/yyyy")
          .option("inferSchema", true)
          .load(urlToJoin.toURI().getPath());
    final Dataset<Row> dataframeJoin = dataframe.join(
          dataframeToJoin,
          dataframeToJoin.col("basic_id").equalTo(dataframe.col("id")),
          "inner"
    );
    final Map<String, String> dTypes = Discovery.discoverDataframe(dataframeJoin);

    assertThat(dataframe).isNotNull();
    assertThat(dataframeJoin).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType.toString());
    assertTrue(dTypes.containsKey("label"));
    assertThat(dTypes.get("label")).isEqualTo(DataTypes.StringType.toString());
    assertTrue(dTypes.containsKey("value"));
    assertThat(dTypes.get("value")).isEqualTo(DataTypes.DoubleType.toString());
    assertTrue(dTypes.containsKey("date"));
    assertThat(dTypes.get("date")).isEqualTo(DataTypes.TimestampType.toString());
    assertTrue(dTypes.containsKey("join_id"));
    assertThat(dTypes.get("join_id")).isEqualTo(DataTypes.IntegerType.toString());
    assertTrue(dTypes.containsKey("join_value"));
    assertThat(dTypes.get("join_value")).isEqualTo(DataTypes.DoubleType.toString());
    assertTrue(dTypes.containsKey("join_date"));
    assertThat(dTypes.get("join_date")).isEqualTo(DataTypes.TimestampType.toString());
  }

  @Test
  void testDiscoveryCalculate() throws URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final URL url = Objects.requireNonNull(cl.getResource("csv/calculate.csv"), "Cannot find file");

    Dataset<Row> dataframe = spark.read()
          .format("csv")
          .option("sep", ";")
          .option("header", true)
          .option("timestampFormat", "dd/MM/yyyy")
          .option("inferSchema", true)
          .load(url.toURI().getPath());
    dataframe = dataframe
          .withColumn("val1_minus_val2",dataframe.col("val1").minus(dataframe.col("val2")))
          .withColumn("val1_equal_val2",dataframe.col("val1").equalTo(dataframe.col("val2")));
    final Map<String, String> dTypes = Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType.toString());
    assertTrue(dTypes.containsKey("val1"));
    assertThat(dTypes.get("val1")).isEqualTo(DataTypes.IntegerType.toString());
    assertTrue(dTypes.containsKey("val2"));
    assertThat(dTypes.get("val2")).isEqualTo(DataTypes.DoubleType.toString());
    assertTrue(dTypes.containsKey("val1_minus_val2"));
    assertThat(dTypes.get("val1_minus_val2")).isEqualTo(DataTypes.DoubleType.toString());
    assertTrue(dTypes.containsKey("val1_equal_val2"));
    assertThat(dTypes.get("val1_equal_val2")).isEqualTo(DataTypes.BooleanType.toString());
  }
}
