package edu.washington.escience.myria.tool;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ini4j.ConfigParser;

/** The class to read Myria configuration file, e.g. deployment.cfg. */
public final class MyriaConfigurationReader extends ConfigParser {

  /** for serialization. */
  private static final long serialVersionUID = 1L;
  /** usage. */
  public static final String USAGE = "java MyriaConfigurationReader [config_file]";

  /**
   * entry point.
   *
   * @param args args.
   * @throws IOException if file system error occurs.
   * */
  public static void main(final String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println(USAGE);
      return;
    }
    MyriaConfigurationReader reader = new MyriaConfigurationReader();
    // config.setEmptyOption(true);
    reader.load(args[0]);
  }

  /**
   * read and parse the config file.
   *
   * @param filename filename.
   * @return parsed mapping from sections to keys to values.
   * @throws IOException if anything during parsing happened.
   * */
  public Map<String, Map<String, String>> load(final String filename) throws IOException {
    File f = new File(filename);
    if (!f.exists()) {
      throw new RuntimeException("config file " + filename + " doesn't exist!");
    }
    Map<String, Map<String, String>> ans = new HashMap<String, Map<String, String>>();
    try {
      this.read(f);
      List<String> sections = sections();
      for (String section : sections) {
        ans.put(section, new HashMap<String, String>());
        List<Map.Entry<String, String>> items = this.items(section);
        for (Map.Entry<String, String> entry : items) {
          ans.get(section).put(entry.getKey(), entry.getValue());
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    String defaultPath = ans.get("deployment").get("path");
    String defaultDatabaseName = ans.get("deployment").get("database_name");
    Map<String, String> workers = ans.get("workers");
    HashMap<String, String> paths = new HashMap<String, String>();
    HashMap<String, String> databaseNames = new HashMap<String, String>();
    for (String workerId : workers.keySet()) {
      String[] tmp = workers.get(workerId).split(":");
      workers.put(workerId, tmp[0] + ":" + tmp[1]);
      if (tmp.length >= 3 && tmp[2] != null && tmp[2].length() > 0) {
        paths.put(workerId, tmp[2]);
      } else {
        paths.put(workerId, defaultPath);
      }
      if (tmp.length >= 4 && tmp[3] != null && tmp[3].length() > 0) {
        databaseNames.put(workerId, tmp[3]);
      } else {
        databaseNames.put(workerId, defaultDatabaseName);
      }
    }
    ans.put("paths", paths);
    ans.put("databaseNames", databaseNames);
    if (ans.get("deployment").get("debug_mode") == null) {
      /* default: false */
      ans.get("deployment").put("debug_mode", "false");
    }
    return ans;
  }
}

