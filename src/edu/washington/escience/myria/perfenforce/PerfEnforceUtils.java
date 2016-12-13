/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.Server;

/**
 * Helper Methods
 */
public class PerfEnforceUtils {

  public static Set<Integer> getWorkerRangeSet(final int limit) {
    Set<Integer> seq = new HashSet<Integer>();
    for (int i = 1; i <= limit; i++) {
      seq.add(i);
    }
    return seq;
  }

  public static int[] getRangeInclusiveArray(final int min, final int max) {
    int numberElements = (max - min) + 1;
    int[] intArray = new int[numberElements];
    for (int i = 0; i < numberElements; i++) {
      intArray[i] = min + i;
    }
    return intArray;
  }

  /*
   * Construct UNION view
   */
  public static String createUnionQuery(final List<RelationKey> keysToUnion) {
    String sql = "";
    for (RelationKey table : keysToUnion) {
      sql +=
          String.format(
              "select * from \"%s:%s:%s\"",
              table.getUserName(),
              table.getProgramName(),
              table.getRelationName());
      if (table != keysToUnion.get(keysToUnion.size() - 1)) {
        sql += " UNION ALL ";
      }
    }
    return sql;
  }

  /*
   * Get list of attributes in a string
   */
  public static String getAttributeKeyString(final Set<Integer> keys, final Schema schema) {
    String keyString = "";
    int counter = 1;
    for (int key : keys) {
      keyString += schema.getColumnName(key);
      if (counter != keys.size()) {
        keyString += ",";
      }
      counter++;
    }
    return keyString;
  }

  /*
   * Get list of attributes in a schema
   */
  public static Schema getAttributeKeySchema(final Set<Integer> keys, final Schema schema) {
    Schema keySchema = Schema.EMPTY_SCHEMA;
    for (int key : keys) {
      keySchema =
          Schema.appendColumn(keySchema, schema.getColumnType(key), schema.getColumnName(key));
    }
    return keySchema;
  }

  public static String getMaxFeature(
      final Server server, final String sqlQuery, final int configuration)
      throws PerfEnforceException, Exception {

    try {
      String explainQuery = "EXPLAIN " + sqlQuery;
      List<String> featuresList = new ArrayList<String>();
      String maxFeature = "";
      double maxCost = Integer.MIN_VALUE;

      String[] allWorkerFeatures =
          server.executeSQLStatement(
              explainQuery,
              Schema.ofFields("explain", Type.STRING_TYPE),
              PerfEnforceUtils.getWorkerRangeSet(configuration));

      for (String currentWorkerFeatures : allWorkerFeatures) {

        //if it matches the following pattern -- change it to a REGEX
        if (currentWorkerFeatures.contains("cost")) {

          currentWorkerFeatures =
              currentWorkerFeatures.substring(currentWorkerFeatures.indexOf("cost"));
          currentWorkerFeatures = currentWorkerFeatures.replace("\"", " ");
          String[] cmd = {
            "sh",
            "-c",
            "echo \""
                + currentWorkerFeatures
                + "\" | sed -e 's/.*cost=//' -e 's/\\.\\./,/' -e 's/ rows=/,/' -e 's/ width=/,/' -e 's/)//'"
          };
          ProcessBuilder pb = new ProcessBuilder(cmd);
          Process p;

          p = pb.start();
          BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
          currentWorkerFeatures = input.readLine();

          if (explainQuery.contains("WHERE")) {
            String[] tables =
                explainQuery
                    .substring(explainQuery.indexOf("FROM"), explainQuery.indexOf("WHERE"))
                    .split(",");
            currentWorkerFeatures = tables.length + "," + currentWorkerFeatures;
          } else {
            currentWorkerFeatures = "1," + currentWorkerFeatures;
          }

          currentWorkerFeatures += "," + configuration + ",0";
          featuresList.add(currentWorkerFeatures);
        }
      }

      for (String f : featuresList) {
        double cost = Double.parseDouble(f.split(",")[2]);
        if (cost > maxCost) {
          maxCost = cost;
          maxFeature = f;
        }
      }

      return maxFeature;

    } catch (Exception e) {
      throw e;
      //throw new PerfEnforceException("Error collecting the max feature");
    }
  }
}
