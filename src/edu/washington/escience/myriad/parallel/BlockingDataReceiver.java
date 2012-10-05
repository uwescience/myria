package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.table._TupleBatch;

public class BlockingDataReceiver extends Operator {

  private final _TupleBatch outputBuffer;
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
