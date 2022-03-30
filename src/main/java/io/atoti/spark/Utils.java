package io.atoti.spark;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import scala.collection.JavaConverters;
import scala.collection.Seq;

class ArrayElement {
  int index;
  long value;

  public ArrayElement(int index, long value) {
    this.index = index;
    this.value = value;
  }
}

public class Utils {
  public static <T> ArrayList<T> convertScalaArrayToArray(Seq<T> arr) {
    return new ArrayList<T>(JavaConverters.asJavaCollectionConverter(arr).asJavaCollection());
  }

  public static Seq<Long> convertToArrayListToScalaArraySeq(List<Long> arr) {
    return JavaConverters.asScalaBuffer(arr).iterator().toSeq();
  }

  public static long t(ArrayElement a, ArrayElement b) {
    return b.value - a.value;
  }

  public static PriorityQueue<ArrayElement> constructMaxHeap(ArrayList<Long> arr) {
    PriorityQueue<ArrayElement> pq =
        new PriorityQueue<ArrayElement>(
            (ArrayElement a, ArrayElement b) -> Long.compare(a.value, b.value));
    pq.addAll(
        IntStream.range(0, arr.size())
            .mapToObj((int k) -> new ArrayElement(k, arr.get(k)))
            .collect(Collectors.toList()));
    return pq;
  }

  public static long quantile(ArrayList<Long> arr, float percent) {
    PriorityQueue<ArrayElement> pq = constructMaxHeap(arr);
    int index = (int) Math.floor(arr.size() * (100 - percent) / 100);

    for (int i = arr.size() - 1; i > index; i--) {
      pq.poll();
    }

    return pq.poll().value;
  }

  public static int quantileIndex(ArrayList<Long> arr, float percent) {
    PriorityQueue<ArrayElement> pq = constructMaxHeap(arr);
    int index = (int) Math.floor(arr.size() * (100 - percent) / 100);

    for (int i = arr.size() - 1; i > index; i--) {
      pq.poll();
    }

    return pq.poll().index;
  }

  public static int findKthLargestElement(ArrayList<Integer> arr, int k) {
    if (k < arr.size()) {
      throw new ArrayIndexOutOfBoundsException();
    }

    PriorityQueue<Integer> pq = new PriorityQueue<Integer>(Comparator.reverseOrder());

    pq.addAll(arr);

    for (int i = 0; i < k; i++) {
      pq.poll();
    }

    return pq.peek();
  }
}
