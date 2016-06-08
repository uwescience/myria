package edu.washington.escience.myria.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Inject delay in processing each TupleBatch.
 */
public class DuplicateTBGenerator extends LeafOperator {

  /**
   * @param tb TupleBatch to duplicate.
   * @param numDuplicates number of duplicates to generate.
   */
  public DuplicateTBGenerator(final TupleBatch tb, final int numDuplicates) {
    this.numDuplicates = numDuplicates;
    this.tb = Preconditions.checkNotNull(tb);
  }

  /**
   * TupleBatch to duplicate.
   */
  private final TupleBatch tb;

  /**
   * Number of duplicates.
   */
  private final int numDuplicates;

  /**
   * @param numDuplicates number of duplicates to generate.
   */
  private int numDuplicated;
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected final void init(final ImmutableMap<String, Object> initProperties) throws DbException {
    numDuplicated = 0;
  }

  @Override
  protected final void cleanup() throws DbException {}

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    if (numDuplicated >= numDuplicates) {
      return null;
    }
    numDuplicated++;
    return tb;
  }

  @Override
  public final Schema generateSchema() {
    return tb.getSchema();
  }
}
