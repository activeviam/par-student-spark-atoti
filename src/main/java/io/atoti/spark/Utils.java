package io.atoti.spark;

import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.Comparator;

import scala.collection.JavaConverters;
import scala.collection.immutable.ArraySeq;

record ArrayElement<T>(int index, T value) {}

public class Utils {

	public static ArrayList<Integer> convertScalaArrayToArray(ArraySeq<Integer> arr) {
		return new ArrayList<Integer>(JavaConverters
				.asJavaCollectionConverter(arr)
				.asJavaCollection()
		);
	}
	
	public static PriorityQueue<ArrayElement<Integer>> constructMaxHeap(ArrayList<Integer> arr) {
		var pq = new PriorityQueue<ArrayElement<Integer>>();
		pq.addAll(IntStream.range(0, arr.size())
				.mapToObj((int k) -> new ArrayElement<Integer>(k, arr.get(k)))
				.collect(Collectors.toList())
			);
		return pq;
	}
	
	public static int findKthLargestElement(ArrayList<Integer> arr, int k) {
		if (k < arr.size()) {
			throw new ArrayIndexOutOfBoundsException();
		}
		
		var pq = new PriorityQueue<Integer>(Comparator.reverseOrder());
		
		pq.addAll(arr);
		
		for (int i = 0; i < k; i++) {
			pq.poll();
		}
	
		return pq.peek();
	}
}
