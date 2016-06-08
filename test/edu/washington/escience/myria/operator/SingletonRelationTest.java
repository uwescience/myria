package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.TestEnvVars;

public class SingletonRelationTest {

  @Test
  public void test() throws DbException {
    SingletonRelation singleton = new SingletonRelation();
    singleton.open(TestEnvVars.get());
    long count = 0;
    while (!singleton.eos()) {
      TupleBatch tb = singleton.nextReady();
      if (tb == null) {
        continue;
      }
      count += tb.numTuples();
    }
    singleton.close();
    assertEquals(1, count);
  }
}
