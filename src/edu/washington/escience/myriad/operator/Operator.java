package edu.washington.escience.myriad.operator;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * Abstract class for implementing operators.
 * 
 * @author slxu
 * 
 *         Currently, the operator api design requires that each single operator instance should be executed within a
 *         single thread.
 * 
 *         No multi-thread synchronization is considered.
 * 
 */
public abstract class Operator implements Serializable {

  /**
   * logger.
   * */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Operator.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * A bit denoting whether the operator is open (initialized).
   * */
  private boolean open = false;

  /**
   * EOS. Initially set it as true;
   * */
  private volatile boolean eos = true;

  /**
   * End of iteration.
   * */
  private boolean eoi = false;

  /**
   * Closes this iterator.
   * 
   * @throws DbException if any errors occur
   */
  public final void close() throws DbException {
    // Ensures that a future call to next() or nextReady() will fail
    // outputBuffer = null;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Operator {} closed, #output TBs: {}, # output tuples: {}", this, numOutputTBs, numOutputTuples);
    }
    open = false;
    eos = true;
    eoi = false;
    cleanup();
    final Operator[] children = getChildren();
    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          child.close();
        }
      }
    }
  }

  /**
   * Check if EOS is meet.
   * 
   * This method is non-blocking.
   * 
   * @return if the Operator is EOS
   * 
   * */
  public final boolean eos() {
    return eos;
  }

  /**
   * @return if the operator received an EOI.
   * */
  public final boolean eoi() {
    return eoi;
  }

  /**
   * Returns the next output TupleBatch, or null if EOS is meet.
   * 
   * This method is blocking.
   * 
   * 
   * @return the next output TupleBatch, or null if EOS
   * 
   * @throws DbException if any processing error occurs
   * @throws InterruptedException if the execution thread is interrupted
   * 
   */
  protected abstract TupleBatch fetchNext() throws DbException, InterruptedException;

  /**
   * @return return the children Operators of this operator. If there is only one child, return an array of only one
   *         element. For join operators, the order of the children is not important. But they should be consistent
   *         among multiple calls.
   */
  public abstract Operator[] getChildren();

  /**
   * Get next TupleBatch. If EOS has not meet, it will wait until a TupleBatch is ready
   * 
   * This method is blocking.
   * 
   * @return next TupleBatch, or null if EOS or EOI
   * 
   * @throws DbException if there's any problem in fetching the next TupleBatch.
   * @throws IllegalStateException if the operator is not open yet
   * @throws InterruptedException if the execution thread is interrupted
   * */
  public final TupleBatch next() throws DbException, InterruptedException {
    if (!open) {
      throw new IllegalStateException("Operator not yet open");
    }
    if (eos() || eoi()) {
      return null;
    }

    LOGGER.error("Error: next get called at non-blocking execution.", new NullPointerException());

    TupleBatch result = null;

    result = fetchNext();

    while (result != null && result.numTuples() <= 0) {
      result = fetchNext();
    }

    if (result == null) {
      // now we have three possibilities when result == null: EOS, EOI, or just a null.
      // returns a null won't cause a problem so far, since a producer will keep calling fetchNext() until EOS
      // call checkEOSAndEOI to set self EOS and EOI, if applicable
      checkEOSAndEOI();
    } else {
      numOutputTBs++;
      numOutputTuples += result.numTuples();
    }
    return result;
  }

  /**
   * process EOS and EOI logic.
   * */
  protected void checkEOSAndEOI() {
    // this is the implementation for ordinary operators, e.g. join, project.
    // some operators have their own logics, e.g. LeafOperator, IDBInput.
    // so they should override this function
    Operator[] children = getChildren();
    childrenEOI = getChildrenEOI();
    boolean hasEOI = false;
    if (children.length > 0) {
      boolean allEOS = true;
      int count = 0;
      for (int i = 0; i < children.length; ++i) {
        if (children[i].eos()) {
          childrenEOI[i] = true;
        } else {
          allEOS = false;
          if (children[i].eoi()) {
            hasEOI = true;
            childrenEOI[i] = true;
            children[i].setEOI(false);
          }
        }
        if (childrenEOI[i]) {
          count++;
        }
      }

      if (allEOS) {
        setEOS();
      }

      if (count == children.length && hasEOI) {
        // only emit EOI if it actually received at least one EOI
        eoi = true;
        Arrays.fill(childrenEOI, false);
      }
    }
  }

  /**
   * Check if currently there's any TupleBatch available for pull.
   * 
   * This method is non-blocking.
   * 
   * If the thread is interrupted during the processing of nextReady, the interrupt status will be kept.
   * 
   * @throws DbException if any problem
   * 
   * @return if currently there's output for pulling.
   * 
   * */
  public final TupleBatch nextReady() throws DbException {
    if (!open) {
      throw new DbException("Operator not yet open");
    }
    if (eos()) {
      return null;
    }

    if (Thread.interrupted()) {
      Thread.currentThread().interrupt();
      return null;
    }

    TupleBatch result = null;
    result = fetchNextReady();
    while (result != null && result.numTuples() <= 0) {
      // XXX while or not while? For a single thread operator, while sounds more efficient generally
      result = fetchNextReady();
    }

    if (result == null) {
      checkEOSAndEOI();
    } else {
      numOutputTBs++;
      numOutputTuples += result.numTuples();
    }

    return result;
  }

  /**
   * A simple statistic. The number of output tuples generated by this Operator.
   * */
  private long numOutputTuples;

  /**
   * A simple statistic. The number of output TBs generated by this Operator.
   * */
  private long numOutputTBs;

  /**
   * open the operator and do initializations.
   * 
   * @param execEnvVars the environment variables of the execution unit.
   * 
   * @throws DbException if any error occurs
   * */
  public final void open(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    // open the children first
    if (open) {
      // XXX Do some error handling to multi-open?
      throw new DbException("Operator already open.");
    }
    final Operator[] children = getChildren();
    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          child.open(execEnvVars);
        }
      }
    }
    eos = false;
    eoi = false;
    numOutputTBs = 0;
    numOutputTuples = 0;
    // do my initialization
    init(execEnvVars);
    open = true;
  }

  /**
   * Mark the end of an iteration.
   * 
   * @param eoi the new value of eoi.
   */
  public final void setEOI(final boolean eoi) {
    this.eoi = eoi;
  }

  /**
   * @return true if this operator is open.
   */
  public final boolean isOpen() {
    return open;
  }

  /**
   * Do the initialization of this operator.
   * 
   * @param execEnvVars execution environment variables
   * @throws DbException if any error occurs
   */
  protected abstract void init(final ImmutableMap<String, Object> execEnvVars) throws DbException;

  /**
   * Do the clean up, release resources.
   * 
   * @throws DbException if any error occurs
   * */
  protected abstract void cleanup() throws DbException;

  /**
   * Generate next output TupleBatch if possible. Return null immediately if currently no output can be generated.
   * 
   * Do not block the execution thread in this method, including sleep, wait on locks, etc.
   * 
   * @throws DbException if any error occurs
   * 
   * @return next ready output TupleBatch. null if either EOS or no output TupleBatch can be generated currently.
   * */
  protected abstract TupleBatch fetchNextReady() throws DbException;

  /**
   * Explicitly set EOS for this operator.
   * 
   * Operators should not be able to unset an already set EOS except reopen it.
   */
  protected final void setEOS() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Operator EOS: " + this);
    }
    eos = true;
  }

  /**
   * @return return the Schema of the output tuples of this operator.
   * 
   */
  public abstract Schema getSchema();

  /**
   * This method is blocking.
   * 
   * @param children the Operators which are to be set as the children(child) of this operator
   */
  // have we ever used this function?
  // May be used soon after operator refactoring.
  public abstract void setChildren(Operator[] children);

  /**
   * Store if the children have meet EOI.
   * */
  private boolean[] childrenEOI = null;

  /**
   * @return children EOI status.
   * */
  protected final boolean[] getChildrenEOI() {
    if (childrenEOI == null) {
      // getChildren() == null indicates a leaf operator, which has its own checkEOSAndEOI()
      childrenEOI = new boolean[getChildren().length];
    }
    return childrenEOI;
  }

  /**
   * For use in leaf operators.
   * */
  protected static final Operator[] NO_CHILDREN = new Operator[] {};

}
