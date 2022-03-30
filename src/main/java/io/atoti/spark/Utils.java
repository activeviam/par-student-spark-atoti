package io.atoti.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.collection.JavaConverters;
import scala.collection.immutable.ArraySeq;
import scala.collection.immutable.Seq;

public class Utils {
  record ArrayElement(int index, long value) {}

  public static ArrayList<Long> convertScalaArrayToArray(ArraySeq<Long> arr) {
    return new ArrayList<Long>(JavaConverters.asJavaCollectionConverter(arr).asJavaCollection());
  }

  public static Seq<Long> convertToArrayListToScalaArraySeq(List<Long> arr) {
    return JavaConverters.asScala(arr).iterator().toSeq();
  }

  public static PriorityQueue<ArrayElement> constructMaxHeap(ArrayList<Long> arr) {
    var pq =
        new PriorityQueue<ArrayElement>(
            (ArrayElement a, ArrayElement b) -> Long.compare(a.value(), b.value()));
    pq.addAll(
        IntStream.range(0, arr.size())
            .mapToObj((int k) -> new ArrayElement(k, arr.get(k)))
            .collect(Collectors.toList()));
    return pq;
  }

  public static long quantile(ArrayList<Long> arr, float percent) {
    var pq = constructMaxHeap(arr);
    int index = (int) Math.floor(arr.size() * (100 - percent) / 100);

    return Stream.generate(pq::poll).limit(arr.size() - index).skip(arr.size() - index - 1).findFirst().orElseThrow().value();
  }

  public static int quantileIndex(ArrayList<Long> arr, float percent) {
    var pq = constructMaxHeap(arr);
    int index = (int) Math.floor(arr.size() * (100 - percent) / 100);

    return Stream.generate(pq::poll).limit(arr.size() - index).skip(arr.size() - index - 1).findFirst().orElseThrow().index();
  }
}
