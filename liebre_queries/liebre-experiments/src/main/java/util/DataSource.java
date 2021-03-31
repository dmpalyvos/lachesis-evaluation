package util;

import common.Named;
import java.util.Queue;

public interface DataSource<T> extends Runnable, Named {

  Queue<T> initQueue(int index);

  void enableQueue(int index);

  void disableQueue(int index);

  @Override
  String getId();

  String streamName(int index);

  @Override
  int getIndex();

  void emit(T tuple);
}
