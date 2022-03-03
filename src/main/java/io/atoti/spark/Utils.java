package io.atoti.spark;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.collection.JavaConverters;
import scala.collection.immutable.ArraySeq;
import scala.collection.immutable.Seq;

record ArrayElement(int index, int value) {}

public class Utils {

	public static ArrayList<Long> convertScalaArrayToArray(ArraySeq<Long> arr) {
		return new ArrayList<Long>(JavaConverters
				.asJavaCollectionConverter(arr)
				.asJavaCollection()
		);
	}

	public static Seq<Long> convertToArrayListToScalaArraySeq(List<Long> arr) {
		return JavaConverters.asScala(arr).iterator().toSeq();
	}
	
	public static PriorityQueue<ArrayElement> constructMaxHeap(ArrayList<Integer> arr) {
		var pq = new PriorityQueue<ArrayElement>(
			(ArrayElement a, ArrayElement b) -> b.value() - a.value()
		);
		pq.addAll(IntStream.range(0, arr.size())
				.mapToObj((int k) -> new ArrayElement(k, arr.get(k)))
				.collect(Collectors.toList())
			);
		return pq;
	}
	
	public static int quantile(ArrayList<Integer> arr, float percent) {
		var pq = constructMaxHeap(arr);
		int index = (int)Math.ceil(arr.size() * percent / 100);
		
		for (int i = arr.size() - 1; i > index; i--) {
			pq.poll();
		}
		
		var k = pq.poll();
		return (k.value() + pq.peek().value()) / 2;
	}
	
	public static int quantileIndex(ArrayList<Integer> arr, float percent) {
		var pq = constructMaxHeap(arr);
		int index = (int)Math.ceil(arr.size() * percent / 100);
		
		for (int i = arr.size() - 1; i > index; i--) {
			pq.poll();
		}
		
		return pq.poll().index();
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
