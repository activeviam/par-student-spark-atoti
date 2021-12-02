package io.atoti.spark.condition;

import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public record OrCondition(List<QueryCondition> conditions) implements QueryCondition {

	public OrCondition {
		if (conditions.isEmpty()) {
			throw new IllegalArgumentException("Cannot accept an empty list of conditions");
		}
	}

	public static OrCondition of(final QueryCondition... conditions) {
		return new OrCondition(List.of(conditions));
	}

	@Override
	public FilterFunction<Row> getCondition() {
		return (Row row) -> this.conditions.stream().anyMatch((condition) -> {
			final var filterFunction = condition.getCondition();
			try {
				return filterFunction.call(row);
			} catch (Exception e) {
				throw new IllegalStateException("Failed to execute condition " + condition, e);
			}
		});
	}

}
