/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;

/**
 * 
 */
public class PerfEnforceConfigurationParser {
  /** Logger. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PerfEnforceConfigurationParser.class);

  public static List<TableDescriptionEncoding> getTablesOfType(final String type, final String configFilePath) {
    List<TableDescriptionEncoding> listTablesOfType = new ArrayList<TableDescriptionEncoding>();
    Gson gson = new Gson();
    String stringFromFile;
    try {
      stringFromFile = Files.toString(new File(configFilePath), Charsets.UTF_8);
      TableDescriptionEncoding[] tableList = gson.fromJson(stringFromFile, TableDescriptionEncoding[].class);

      for (TableDescriptionEncoding currentTable : tableList) {
        if (currentTable.type.equals(type)) {
          listTablesOfType.add(currentTable);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return listTablesOfType;
  }
}
