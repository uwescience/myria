/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.washington.escience.myria.RelationKey;

/**
 * Helper Methods
 */
public class PerfEnforceUtils {
  public static Set<Integer> getRangeSet(final int limit) {
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
          String.format("select * from \"%s:%s:%s\"", table.getUserName(), table.getProgramName(), table
              .getRelationName());
      if (table != keysToUnion.get(keysToUnion.size() - 1)) {
        sql += " UNION ALL ";
      }
    }
    return sql;
  }
}
