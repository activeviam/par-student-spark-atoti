package io.atoti.spark;

import java.util.List;

public class ListQuery {

	/**
	 * Retrieves a series of rows from a dataframe.
	 * @param wantedColumns names of the columns we want to see in the resulting list
	 * @param limit max number of rows to return. Passing a negative number disable this option
	 * @param offset offset at which this starts reading the input dataframe
	 * @return list of rows extracted from the DataFrame
	 */
	public static List<Object> list(Object dataframe, List<String> wantedColumns, int limit, int offset) {
		throw new UnsupportedOperationException("TODO");
	}

}
