package util;


import org.apache.flink.metrics.Gauge;

public class AverageGauge implements Gauge<Double> {

  private int count;
  private double sum;

  public synchronized void add(double value) {
    sum += value;
    count += 1;
  }

  @Override
  public synchronized Double getValue() {
    final double result = count > 0 ? sum / count : -1;
    sum = count = 0;
    return result;
  }
}
