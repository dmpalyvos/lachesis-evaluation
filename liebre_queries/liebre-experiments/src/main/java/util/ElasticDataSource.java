package util;

import component.source.SourceFunction;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import synthetic.CloneableSourceFunction;

public class ElasticDataSource<T> extends AbstractDataSource<T> {

  private final Map<Integer, Queue<T>> queues = new HashMap<>();
  private final Set<Integer> disabledIndexes = ConcurrentHashMap.newKeySet();

  public ElasticDataSource(String id, CloneableSourceFunction<T> function, BitSet affinity) {
    super(function, id, affinity);
  }

  @Override
  public Queue<T> initQueue(int index) {
    Queue<T> queue = new ConcurrentLinkedDeque<>();
    queues.put(index, queue);
    return queue;
  }

  @Override
  public void enableQueue(int index) {
    disabledIndexes.remove(index);
  }

  @Override
  public void disableQueue(int index) {
    disabledIndexes.add(index);
  }

  @Override
  public void emit(T tuple) {
    for (Map.Entry<Integer, Queue<T>> mapEntry : queues.entrySet()) {
      if (disabledIndexes.contains(mapEntry.getKey())) {
        continue;
      }
      mapEntry.getValue().add(tuple);
    }
  }
}
