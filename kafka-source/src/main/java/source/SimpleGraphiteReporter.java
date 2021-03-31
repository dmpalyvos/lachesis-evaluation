package source;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleGraphiteReporter {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleGraphiteReporter.class);
  private final int graphitePort;
  private final String graphiteHost;
  private Socket socket;
  private DataOutputStream output;

  public SimpleGraphiteReporter(String graphiteHost, int graphitePort) {
    this.graphiteHost = graphiteHost;
    this.graphitePort = graphitePort;
  }

  public void open() {
    try {
      socket = new Socket(graphiteHost, graphitePort);
      output = new DataOutputStream(socket.getOutputStream());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void report(long timestampSeconds, String key, Object value) {
    try {
      String message = key + " " + value + " " + timestampSeconds + "\n";
      output.writeBytes(message);
    }
    catch (IOException exception) {
      LOG.warn("Failed to report to graphite: {}", exception.getMessage());
    }
  }

  public void close() {
    try {
      output.flush();
      output.close();
      socket.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

}
