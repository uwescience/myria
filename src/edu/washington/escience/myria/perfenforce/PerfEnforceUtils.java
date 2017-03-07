/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.Server;

/**
 * Helper Methods for PerfEnforce
 */
public class PerfEnforceUtils {

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PerfEnforceUtils.class);

  /**
   * Returns a subset of workers
   *
   * @param limit the upper bound for the range of workers
   * @return the resulting set of worker ids
   */
  public static Set<Integer> getWorkerRangeSet(final int limit) {
    Set<Integer> seq = new HashSet<Integer>();
    for (int i = 1; i <= limit; i++) {
      seq.add(i);
    }
    return seq;
  }

  /**
   * Returns an array of worker ids
   *
   * @param min the min worker id (inclusive)
   * @param max the max worker id (inclusive)
   * @return the resulting array of worker ids
   */
  public static int[] getRangeInclusiveArray(final int min, final int max) {
    int numberElements = (max - min) + 1;
    int[] intArray = new int[numberElements];
    for (int i = 0; i < numberElements; i++) {
      intArray[i] = min + i;
    }
    return intArray;
  }

  /**
   * Creates a UNION sql statement based on a set of relationKeys
   *
   * @param keysToUnion the relationKeys to union
   * @return the UNION sql statement across a set of relations
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

  /**
   * Concatenates an array of attributes in a string
   *
   * @param keys the set of columns to concatenate
   * @param schema the schema of the relation
   * @return the string representation of the schema separated by commas
   *
   */
  public static String getAttributeKeyString(final Set<Integer> keys, final Schema schema) {
    return Joiner.on(",").join(getAttributeKeySchema(keys, schema).getColumnNames());
  }

  /**
   * Creates a new schema based on the given column ids
   *
   * @param keys the set of columns to concatenate
   * @param schema the schema to create
   * @return the schema for a subset of columns
   */
  public static Schema getAttributeKeySchema(final Set<Integer> keys, final Schema schema) {
    return schema.getSubSchema(Ints.toArray(keys));
  }

  /**
   * This method runs a SQL worker on a subset of workers. It will return the features with the highest cost to account for skew (if any)
   *
   * @param server an instance of the server
   * @param sqlQuery the SQL statement to run
   * @param configuration the configuration
   * @return the features with the highest cost
   * @throws PerfEnforceException
   */
  public static String getMaxFeature(final Server server, String sqlQuery, final int configuration)
      throws PerfEnforceException {

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
        if (currentWorkerFeatures.contains("cost")) {
          currentWorkerFeatures =
              currentWorkerFeatures.substring(currentWorkerFeatures.indexOf("cost"));
          currentWorkerFeatures =
              currentWorkerFeatures
                  .replace("\"", " ")
                  .replaceAll(".*cost=|\\)", "")
                  .replaceAll("\\.\\.| rows=| width=", ",");

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
      throw new PerfEnforceException("Error collecting the max feature");
    }
  }
}
