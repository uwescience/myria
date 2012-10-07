package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Blocking when receiving data from children.
 * */
public final class BlockingDataReceiver extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * output buffer.
   * */
  private final _TupleBatch outputBuffer;

  /**
   * child operator.
   * */
  private final Operator child;

  public BlockingDataReceiver(final _TupleBatch outputBuffer, final Operator child) {
    this.outputBuffer = outputBuffer;
    this.child = child;
  }

  @Override
  protected _TupleBatch fetchNext() throws DbException {
    while (this.child.hasNext()) {
      this.outputBuffer.append(this.child.next());
    }
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  public _TupleBatch getOutputBuffer() {
    return this.outputBuffer;
  }

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void open() throws DbException {
    if (child != null) {
      child.open();
    }
    super.open();
  }

  @Override
  public void setChildren(final Operator[] children) {
    // TODO using reflection
    throw new UnsupportedOperationException();
  }

}
