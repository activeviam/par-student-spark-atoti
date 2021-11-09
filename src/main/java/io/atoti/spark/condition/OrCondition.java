package io.atoti.spark.condition;

import java.util.List;

public record OrCondition(List<QueryCondition> conditions) implements QueryCondition {

	public OrCondition {
		if (conditions.isEmpty()) {
			throw new IllegalArgumentException("Cannot accept an empty list of conditions");
		}
	}

	public static OrCondition of(final QueryCondition... conditions) {
		return new OrCondition(List.of(conditions));
	}

}
