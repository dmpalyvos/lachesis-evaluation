package util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.commons.lang3.Validate;

public class SamplingTimestampedRecorder {

  private final PrintWriter writer;
  private final int sampleEvery;
  private int sampleCounter = 0;

  public SamplingTimestampedRecorder(String outputFile, int sampleEvery) {
    Validate.notEmpty(outputFile, "outputFile");
    Validate.isTrue(sampleEvery > 0, "sampleEvery <= 0");
    this.sampleEvery = sampleEvery;
    try {
      this.writer = new PrintWriter(new FileWriter(outputFile), true);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void add(long v) {
    if (++sampleCounter == sampleEvery) {
      final long thisSec = System.currentTimeMillis() / 1000;
      writer.println(thisSec + "," + v);
      sampleCounter = 0;
    }
  }

}
