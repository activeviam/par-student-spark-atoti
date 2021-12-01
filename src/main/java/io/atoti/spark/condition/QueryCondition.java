package io.atoti.spark.condition;

public sealed interface QueryCondition permits
				AndCondition, OrCondition, NotCondition,
		EqualCondition, NullCondition,
	TrueCondition, FalseCondition {
}
