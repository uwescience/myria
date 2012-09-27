package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

public class BlockingDataReceiver extends Operator {

  private final _TupleBatch outputBuffer;
  private final Operator child;

  public BlockingDataReceiver(_TupleBatch outputBuffer, Operator child) {
    this.outputBuffer = outputBuffer;
    this.child = child;
  }

  public void open() throws DbException {
    if (child != null)
      child.open();
    super.open();
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

  @Override
  public Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public void setChildren(Operator[] children) {
    // TODO using reflection
    throw new UnsupportedOperationException();
  }

  public _TupleBatch getOutputBuffer() {
    return this.outputBuffer;
  }

}
