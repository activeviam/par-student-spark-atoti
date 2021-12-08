package io.atoti.spark;

import org.apache.spark.sql.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BenchmarkListQuery {

    static SparkSession spark = SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();
    static Dataset<Row> dataframe = CsvReader.read("csv/US_accidents_Dec20_updated.csv", spark, ",");
    static int limit = 100000;
    static int offset = 100000;
    static List<String> wantedColumns = List.of("ID", "Severity");

    public BenchmarkListQuery() {
        spark.sparkContext().setLogLevel("ERROR");
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void BenchmarkColumnId(Blackhole bh) {
        final List<Row> rows = ListQuery.list(dataframe, wantedColumns, limit, offset);
        bh.consume(rows);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void BenchmarkLimitAndTail(Blackhole bh) {
        List<Row> rows;

        if (offset < 0) {
            throw new IllegalArgumentException("Cannot accept a negative offset");
        }

        final Column[] columns = wantedColumns.stream().map(functions::col).toArray(Column[]::new);
        final int dfSize = (int) dataframe.count();
        bh.consume(dfSize);

        if(limit >= 0) {
            rows = Arrays.asList((Row[]) dataframe.select(columns).limit(limit + offset).tail(
                    limit + offset <= dfSize ? limit : Math.max(0, dfSize - offset)
            ));
        } else {
            rows = Arrays.asList((Row[]) dataframe.select(columns).tail(Math.max(dfSize - offset, 0)));
        }
        bh.consume(rows);
    }
}
