/**
 *
 */
package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

/**
 * Tests that crossing with a singleton works and catches the major error cases.
 */
public class CrossWithSingletonTest {
  private final int NUM_TUPLES = TupleBatch.BATCH_SIZE * 3 + 3;
  private TupleSource dataSource = null;
  private TupleBatch singleton = null;

  @Before
  public void setUp() {
    Schema dataSchema = Schema.ofFields("val1", Type.LONG_TYPE, "val2", Type.STRING_TYPE);
    TupleBatchBuffer tbb = new TupleBatchBuffer(dataSchema);
    for (int i = 0; i < NUM_TUPLES; ++i) {
      tbb.putLong(0, i);
      tbb.putString(1, "" + i);
    }
    dataSource = new TupleSource(tbb.getAll());

    tbb = new TupleBatchBuffer(Schema.ofFields(Type.DATETIME_TYPE, Type.BOOLEAN_TYPE));
    tbb.putDateTime(0, DateTime.now());
    tbb.putBoolean(1, false);
    singleton = tbb.popAny();
  }

  @SuppressWarnings("deprecation")
  private void verifyMatch(final CrossWithSingleton cross) throws DbException {
    cross.open(TestEnvVars.get());

    int numTuples = 0;
    while (!cross.eos()) {
      TupleBatch next = cross.nextReady();
      if (next == null) {
        continue;
      }
      for (int j = 0; j < next.numTuples(); ++j) {
        assertEquals(j + numTuples, next.getLong(0, j));
        assertEquals("" + (j + numTuples), next.getString(1, j));
        for (int k = 0; k < singleton.numColumns(); ++k) {
          assertEquals(next.getObject(2 + k, j), singleton.getObject(k, 0));
        }
      }
      numTuples += next.numTuples();
    }
  }

  @Test
  public void testWithSingleton() throws DbException {
    TupleSource singletonSource = new TupleSource(singleton);
    CrossWithSingleton cross = new CrossWithSingleton(dataSource, singletonSource);
    assertEquals(Schema.merge(dataSource.getSchema(), singleton.getSchema()), cross.getSchema());
    verifyMatch(cross);
  }

  @Test(expected = IllegalStateException.class)
  public void testWithSingletonWrongSide() throws DbException {
    TupleSource singletonSource = new TupleSource(singleton);
    CrossWithSingleton cross = new CrossWithSingleton(singletonSource, dataSource);
    assertEquals(Schema.merge(singleton.getSchema(), dataSource.getSchema()), cross.getSchema());
    verifyMatch(cross);
  }

  @Test(expected = IllegalStateException.class)
  public void testWithTwoSingletons() throws DbException {
    TupleBatchBuffer tbb = new TupleBatchBuffer(singleton.getSchema());
    tbb.appendTB(singleton);
    tbb.appendTB(singleton);
    TupleSource source = new TupleSource(tbb.getAll());
    CrossWithSingleton cross = new CrossWithSingleton(dataSource, source);
    assertEquals(Schema.merge(dataSource.getSchema(), singleton.getSchema()), cross.getSchema());
    verifyMatch(cross);
  }

  @Test(expected = IllegalStateException.class)
  public void testWithEmptyRelation() throws DbException {
    CrossWithSingleton cross =
        new CrossWithSingleton(dataSource, EmptyRelation.of(singleton.getSchema()));
    verifyMatch(cross);
  }
}
