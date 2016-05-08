/**
 *
 */
package edu.washington.edu.escience.myria.perfenforce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;

/**
 * 
 */
public class PerfEnforceConfigurationParser {

  String configFilePath;

  public PerfEnforceConfigurationParser(final String configFilePath) {
    this.configFilePath = configFilePath;
  }

  public List<TableDescriptionEncoding> getTablesOfType(final String type) throws IOException {
    List<TableDescriptionEncoding> listTablesOfType = new ArrayList<TableDescriptionEncoding>();
    Gson gson = new Gson();
    String stringFromFile = Files.toString(new File(configFilePath), Charsets.UTF_8);;
    TableDescriptionEncoding[] tableList = gson.fromJson(stringFromFile, TableDescriptionEncoding[].class);

    for (TableDescriptionEncoding currentTable : tableList) {
      if (currentTable.type.equals(type)) {
        listTablesOfType.add(currentTable);
      }
    }
    return listTablesOfType;
  }
}
