package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

public final class BlockingDataReceiver extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private final _TupleBatch outputBuffer;
  private Operator child;

  public BlockingDataReceiver(final _TupleBatch outputBuffer, final Operator child) {
    this.outputBuffer = outputBuffer;
    this.child = child;
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    _TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      outputBuffer.append(tb);
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  public _TupleBatch getOutputBuffer() {
    return outputBuffer;
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

}
