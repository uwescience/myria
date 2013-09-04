package edu.washington.escience.myria.operator;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.parallel.TaskResourceManager;

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
   * the name of the operator from json queries (or set from hand-constructed query plans).
   * */
  private String opName = "";

  /**
   * A bit denoting whether the operator is open (initialized).
   * */
  private boolean open = false;

  /**
   * A bit denoting whether the operator has began to consume tuples.
   */
  private boolean startProcessing = false;

  /**
   * EOS. Initially set it as true;
   * */
  private volatile boolean eos = true;

  /**
   * End of iteration.
   * */
  private boolean eoi = false;

  /**
   * Actual execution time.
   */
  private long executionTime = 0;

  /**
   * Environmental variables during execution.
   */
  private ImmutableMap<String, Object> execEnvVars;

  /**
   * @return return environmental variables
   */
  public ImmutableMap<String, Object> getExecEnvVars() {
    return execEnvVars;
  }

  /**
   * @return return query id.
   */
  public long getQueryId() {
    return ((TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER)).getOwnerTask()
        .getOwnerQuery().getQueryID();
  }

  /**
   * @return return profiling mode.
   */
  public boolean isProfilingMode() {
    return ((TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER)).getOwnerTask()
        .getOwnerQuery().isProfilingMode();
  }

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
    Exception errors = null;
    try {
      cleanup();
    } catch (DbException | RuntimeException e) {
      errors = e;
    } catch (Throwable e) {
      errors = new DbException(e);
    }
    final Operator[] children = getChildren();
    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          try {
            child.close();
          } catch (Throwable e) {
            if (errors != null) {
              errors.addSuppressed(e);
            } else {
              if (e instanceof DbException || e instanceof RuntimeException) {
                errors = (Exception) e;
              } else {
                errors = new DbException(e);
              }
            }
          }
        }
      }
    }
    if (errors != null) {
      if (errors instanceof RuntimeException) {
        throw (RuntimeException) errors;
      } else {
        throw (DbException) errors;
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
   * @return return the children Operators of this operator. If there is only one child, return an array of only one
   *         element. For join operators, the order of the children is not important. But they should be consistent
   *         among multiple calls.
   */
  public abstract Operator[] getChildren();

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
    if (eos() || eoi()) {
      return null;
    }

    if (Thread.interrupted()) {
      Thread.currentThread().interrupt();
      return null;
    }

    // record start time
    long startTime = System.currentTimeMillis();

    TupleBatch result = null;
    try {
      if (!startProcessing) {
        if (isProfilingMode()) {
          LOGGER.info("[{}#{}]{}[{}]:begin to process", MyriaConstants.EXEC_ENV_VAR_QUERY_ID, getQueryId(),
              getOpName(), this);
        }
        startProcessing = true;
      }

      result = fetchNextReady();
      while (result != null && result.numTuples() <= 0) {
        // XXX while or not while? For a single thread operator, while sounds more efficient generally
        result = fetchNextReady();
      }
    } catch (RuntimeException | DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException(e);
    }

    if (result == null) {
      checkEOSAndEOI();
    } else {
      numOutputTBs++;
      numOutputTuples += result.numTuples();
      executionTime += System.currentTimeMillis() - startTime;
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
    this.execEnvVars = execEnvVars;
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
    try {
      init(execEnvVars);
    } catch (DbException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException(e);
    }
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
   * @throws Exception if any error occurs
   */
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
  };

  /**
   * Do the clean up, release resources.
   * 
   * @throws Exception if any error occurs
   * */
  protected void cleanup() throws Exception {
  };

  /**
   * Generate next output TupleBatch if possible. Return null immediately if currently no output can be generated.
   * 
   * Do not block the execution thread in this method, including sleep, wait on locks, etc.
   * 
   * @throws Exception if any error occurs
   * 
   * @return next ready output TupleBatch. null if either EOS or no output TupleBatch can be generated currently.
   * */
  protected abstract TupleBatch fetchNextReady() throws Exception;

  /**
   * Explicitly set EOS for this operator.
   * 
   * Operators should not be able to unset an already set EOS except reopen it.
   */
  protected final void setEOS() {
    if (startProcessing && isProfilingMode()) {
      LOGGER.info("[{}#{}]{}[{}]:End of Processing (EOS)", MyriaConstants.EXEC_ENV_VAR_QUERY_ID, getQueryId(),
          getOpName(), this);
      LOGGER.info("[{}#{}]{}[{}]: execution time {} ms", MyriaConstants.EXEC_ENV_VAR_QUERY_ID, getQueryId(),
          getOpName(), this, executionTime);
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

  /**
   * set op name.
   * 
   * @param name op name
   */
  public void setOpName(final String name) {
    opName = name;
  }

  /**
   * get op name.
   * 
   * @return op name
   */
  public String getOpName() {
    return opName;
  }

}
