package edu.washington.escience.myriad.operator;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * An abstract class used to make those specialized operators that only consume tuples simpler to implement.
 * 
 * @author dhalperi
 * 
 */
public abstract class RootOperator extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Source of the tuples to be consumed. */
  private final Operator child;
  /** The ExecutorService for this process. */
  private final ExecutorService executor;
  /** The task that gets run. */
  private Future<?> task;

  /**
   * Perform the function of this operator on the provided tuples. For instance, may print the tuples to the screen or
   * write them to disk.
   * 
   * @param tuples the tuples to be consumed.
   * @throws DbException if there's an error in the database.
   */
  protected abstract void consumeTuples(_TupleBatch tuples) throws DbException;

  /**
   * @return the source of the tuples that this Root operator consumes.
   */
  public final Operator getChild() {
    return child;
  }

  /**
   * Sets important parameters for successful operation.
   * 
   * @param child the source of tuples that this Root operator consumes.
   * @param executor the executor service that controls threads for this process.
   */
  public RootOperator(final Operator child, final ExecutorService executor) {
    this.child = child;
    this.executor = executor;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public final void setChildren(final Operator[] children) {
    if (children.length != 1) {
      throw new IllegalArgumentException("a root operator must have exactly one child");
    }
  }

  @Override
  protected _TupleBatch fetchNext() {
    return null;
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    return null;
  }

  @Override
  public void init() throws DbException {
    task = executor.submit(new CollectTuplesTask());
  }

  @Override
  public void cleanup() {
    try {
      task.get();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public final Schema getSchema() {
    return child.getSchema();
  }

  /**
   * A helper task that gets tuples from the children and then calls the consumeTuples function that the client uses to
   * do something with them.
   * 
   * @author dhalperi
   * 
   */
  class CollectTuplesTask implements Runnable {
    @Override
    public void run() {
      try {
        _TupleBatch tup = null;
        while ((tup = child.next()) != null) {
          consumeTuples(tup);
        }
      } catch (DbException e) {
        e.printStackTrace();
      }
    }
  }

}
