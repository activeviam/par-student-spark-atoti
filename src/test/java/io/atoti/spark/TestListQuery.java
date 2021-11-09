package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TestListQuery {

  @Test
  void testListAllDataFrame() {
    final Object dataframe = null; // from basic.csv
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), -1, 0);
    assertThat(rows).hasSize(3);
    final var valuesById =
        rows.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    row -> ((Number) readRowValue(row, "id")).longValue(),
                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
    assertThat(valuesById).containsExactlyEntriesOf(Map.of(1L, 12.34d, 2L, 13.57d, 3L, -420d));
  }

  @Test
  void testListFirstRows() {
    final Object dataframe = null; // from basic.csv
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 2, 0);
    assertThat(rows).hasSize(2);
    final var valuesById =
        rows.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    row -> ((Number) readRowValue(row, "id")).longValue(),
                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
    assertThat(valuesById).containsExactlyEntriesOf(Map.of(1L, 12.34d, 2L, 13.57d));
  }

  @Test
  void testListLastRow() {
    final Object dataframe = null; // from basic.csv
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 1, 2);
    assertThat(rows).hasSize(1);
    final var valuesById =
        rows.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    row -> ((Number) readRowValue(row, "id")).longValue(),
                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
    assertThat(valuesById).containsExactlyEntriesOf(Map.of(3L, 13.57d));
  }

  static Object readRowValue(final Object row, final String column) {
    throw new UnsupportedOperationException("TODO");
  }
}
