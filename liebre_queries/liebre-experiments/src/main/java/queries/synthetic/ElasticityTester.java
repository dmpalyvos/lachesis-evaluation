package queries.synthetic;

import common.util.Util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import query.Query;
import util.ExperimentSettings;

public class ElasticityTester {
  protected static final Logger LOG = LogManager.getLogger();

  private static final long FREQUENCY = 5000;

  public void run(
      ExperimentSettings settings,
      Query q,
      String id,
      Consumer<Query> queryIncreaser,
      Consumer<Query> queryDecreaser) {
    new Thread(new ElasticityRunner(q, id, queryIncreaser, queryDecreaser, settings)).start();
  }

  private static class ElasticityRunner implements Runnable {
    private final Query q;
    private final Consumer<Query> increaser;
    private final Consumer<Query> decreaaser;
    private final ExperimentSettings settings;
    private final String id;

    public ElasticityRunner(
        Query q,
        String id,
        Consumer<Query> increaser,
        Consumer<Query> decreaaser,
        ExperimentSettings settings) {
      this.q = q;
      this.increaser = increaser;
      this.decreaaser = decreaaser;
      this.settings = settings;
      this.id = id;
    }

    @Override
    public void run() {
      Util.sleep(5000);
      LOG.info("Starting elasticity tester...");
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() < startTime + settings.durationMillis()) {
        int tasksToAdd = numberTasks("ADD");
        int tasksToRemove = numberTasks("REMOVE");
        for (int i = 0; i < tasksToAdd; i++) {
          increaser.accept(q);
        }
        for (int i = 0; i < tasksToRemove; i++) {
          decreaaser.accept(q);
        }
        Util.sleep(FREQUENCY);
      }
      LOG.info("Stopping elasticity runner...");
    }

    private File actionFile(String action) {
      return new File(String.format("elasticity_%s_%s.tmp", id, action));
    }

    private int numberTasks(String action) {
      File actionFile = actionFile(action);
      if (!actionFile.exists()) {
        return 0;
      }
      try {
        BufferedReader br = new BufferedReader(new FileReader(actionFile));
        actionFile.delete();
        return Integer.valueOf(br.readLine().trim());
      } catch (Exception e) {
        LOG.error("Failed to read elasticity configuration: {}", e.getMessage());
        LOG.debug(e);
        return 0;
      }
    }
  }
}
