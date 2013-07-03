package edu.washington.escience.myriad.tool;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ini4j.ConfigParser;

public final class MyriaConfigurationReader extends ConfigParser {

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
  public Map<String, HashMap<String, String>> load(final String filename) throws IOException {
    Map<String, HashMap<String, String>> ans = new HashMap<String, HashMap<String, String>>();
    try {
      this.read(new File(filename));
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
    return ans;
  }
}
