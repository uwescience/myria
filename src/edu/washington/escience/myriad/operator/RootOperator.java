package edu.washington.escience.myriad.operator;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * An abstract class used to make those specialized operators that only consume tuples simpler to implement.
 * 
 * @author dhalperi
 * 
 */
public abstract class RootOperator extends Operator {

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
        TupleBatch tup = null;
        while ((tup = child.next()) != null) {
          consumeTuples(tup);
        }
      } catch (final DbException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Source of the tuples to be consumed. */
  private final Operator child;
  /** The ExecutorService for this process. */
  private ExecutorService executor;

  /** The task that gets run. */
  private Future<?> task;
  /** True if I created my own executor. */
  private boolean myExecutor;

  /**
   * Sets important parameters for successful operation.
   * 
   * @param child the source of tuples that this Root operator consumes.
   * @param executor the executor service that controls threads for this process.
   */
  public RootOperator(final Operator child, final ExecutorService executor) {
    this.child = child;
    this.executor = executor;
    myExecutor = false;
  }

  /**
   * Perform the function of this operator on the provided tuples. For instance, may print the tuples to the screen or
   * write them to disk.
   * 
   * @param tuples the tuples to be consumed.
   * @throws DbException if there's an error in the database.
   */
  protected abstract void consumeTuples(TupleBatch tuples) throws DbException;

  /**
   * @return the source of the tuples that this Root operator consumes.
   */
  public final Operator getChild() {
    return child;
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

  /**
   * Required for serialization---be able to set the executor at the worker.
   * 
   * @param executor the executor service that controls threads for this process.
   */
  public final void setExecutorService(final ExecutorService executor) {
    Objects.requireNonNull(executor);
    if (executor != null) {
      throw new IllegalStateException("RootOperator already has an Executor");
    }
    this.executor = executor;
  }

  private void startAndWaitChild() {
    if (executor == null) {
      executor = Executors.newSingleThreadExecutor();
      myExecutor = true;
    }
    task = executor.submit(new CollectTuplesTask());
    try {
      task.get();
    } catch (final ExecutionException e) {
      e.printStackTrace();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (myExecutor) {
      executor.shutdown();
    }
  }

  @Override
  protected TupleBatch fetchNext() {
    startAndWaitChild();
    return null;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    startAndWaitChild();
    setEOS(true);
    return null;
  }

  @Override
  public final Schema getSchema() {
    return child.getSchema();
  }
}
