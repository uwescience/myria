package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class OrderedDupElimTest {
  @Test
  public void testOrderedDupElim() throws DbException {
    TupleBatchBuffer randomTuples = TestUtils.generateRandomTuples(52300, 5000, true);
    TupleSource child = new TupleSource(randomTuples);
    OrderedDupElim dupElim = new OrderedDupElim(child);
    int count = 0;

    /* Count the dupelim */
    dupElim.open(null);
    while (!dupElim.eos()) {
      TupleBatch tb = dupElim.nextReady();
      if (tb == null) {
        continue;
      }
      count += tb.numTuples();
    }
    dupElim.close();

    /* Count the real answer */
    Map<Long, Set<String>> map = new HashMap<Long, Set<String>>();
    for (TupleBatch tuples : randomTuples.getAll()) {
      for (int i = 0; i < tuples.numTuples(); ++i) {
        Set<String> set = map.get(tuples.getLong(0, i));
        if (set == null) {
          set = new HashSet<String>();
          map.put(tuples.getLong(0, i), set);
        }
        set.add(tuples.getString(1, i));
      }
    }
    int realCount = 0;
    for (Set<String> set : map.values()) {
      realCount += set.size();
    }

    assertEquals(count, realCount);
  }
}
