package source;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

class Config {

  public static final String DUMMY_INPUT_FILE = "DUMMY";

  static Config parse(String[] args) {
    Config config = new Config();
    JCommander.newBuilder()
        .addObject(config)
        .acceptUnknownOptions(true)
        .build().parse(args);
    Validate.isTrue(StringUtils.isAnyBlank(config.inputFile, config.configFile) &&
            !StringUtils.isAllBlank(config.inputFile, config.configFile),
        "Please provide either --configFile or --inputFile");
    if (StringUtils.isBlank(config.inputFile)) {
      config.inputFile = getPathFromJsonConfig(config);
    }
    return config;
  }

  private static String getPathFromJsonConfig(Config config) {
    Gson gson = new Gson();
    try {
      Map<String, Object> json = gson
          .fromJson(new JsonReader(new FileReader(config.configFile)),
              new TypeToken<Map<String, Object>>() {}.getType());
      return (String) json.get("dataset");
    } catch (FileNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Parameter(names = "--inputFile")
  String inputFile;

  @Parameter(names = "--configFile")
  String configFile;

  @Parameter(names = "--graphiteHost", required = true)
  String graphiteHost;

  @Parameter(names = "--graphitePort")
  int graphitePort = 2003;

  @Parameter(names = "--period", description = "Period in ms")
  int period = 100;

  @Parameter(names = "--rate", required = true)
  int rate;

  @Parameter(names = "--offset")
  int offset = 0;

  @Parameter(names = "--stride")
  int stride = 1;

  @Parameter(names = "--topic")
  String topic = "experiment";

  public boolean hasDummyInputFile() {
    return DUMMY_INPUT_FILE.equals(inputFile);
  }

  private Config() {

  }
}
