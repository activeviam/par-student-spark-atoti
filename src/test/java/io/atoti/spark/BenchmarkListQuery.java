package io.atoti.spark;

import org.apache.spark.sql.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(1)
public class BenchmarkListQuery {

    SparkSession spark;
    Dataset<Row> dataframe;
    int limit;
    int offset;
    List<String> wantedColumns;

    @Setup()
    public void setup() {
        spark = SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        dataframe = CsvReader.read("csv/US_accidents_Dec20_updated.csv", spark, ",");
        limit = 100000;
        offset = 100000;
        wantedColumns = List.of("ID", "Severity");
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
    public void BenchmarkColumnId(Blackhole bh) {
        final List<Row> rows = ListQuery.list(dataframe, wantedColumns, limit, offset);
        bh.consume(rows);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
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
