package edu.washington.escience.myria.operator;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.LocalFragment;
import edu.washington.escience.myria.parallel.LocalFragmentResourceManager;
import edu.washington.escience.myria.parallel.LocalSubQuery;
import edu.washington.escience.myria.parallel.SubQueryId;
import edu.washington.escience.myria.parallel.Worker;
import edu.washington.escience.myria.parallel.WorkerSubQuery;
import edu.washington.escience.myria.profiling.ProfilingLogger;
import edu.washington.escience.myria.storage.TupleBatch;

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
   * logger for this class.
   */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Operator.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * the name of the operator from json queries (or set from hand-constructed query plans).
   */
  private String opName = "";

  /**
   * The unique operator id.
   */
  private Integer opId;

  /**
   * A bit denoting whether the operator is open (initialized).
   */
  private boolean open = false;

  /**
   * The {@link Schema} of the tuples produced by this {@link Operator}.
   */
  private Schema schema;

  /**
   * End of stream (EOS). Initialized to true.
   */
  private volatile boolean eos = true;

  /**
   * End of iteration (EOI).
   */
  private boolean eoi = false;

  /**
   * Environmental variables during execution.
   */
  private ImmutableMap<String, Object> execEnvVars;

  /**
   * @JORTIZ: This is super hacky, but I needed the operator to know which worker it belongs to.
   */
  private Worker worker;

  /**
   * @return worker the worker that is running this operator, will this have issues with the subqueries?
   */
  public Worker getWorker() {
    return worker;
  }

  /**
   * @return return environmental variables
   */
  public ImmutableMap<String, Object> getExecEnvVars() {
    return execEnvVars;
  }

  /**
   * Logger for profiling.
   */
  private ProfilingLogger profilingLogger;

  /**
   * Cache for profiling mode.
   */
  private Set<ProfilingMode> profilingMode;

  /**
   * @return the profilingLogger
   */
  public ProfilingLogger getProfilingLogger() {
    Preconditions.checkNotNull(profilingLogger);
    return profilingLogger;
  }

  /**
   * @return return subquery id.
   */
  public SubQueryId getSubQueryId() {
    return ((LocalFragmentResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER))
        .getFragment().getLocalSubQuery().getSubQueryId();
  }

  /**
   * @return the executing {@link LocalSubQuery} that this {@link Operator} is part of.
   */
  public final LocalSubQuery getLocalSubQuery() {
    LocalFragment qstt = getFragment();
    if (qstt == null) {
      return null;
    } else {
      return qstt.getLocalSubQuery();
    }
  }

  /**
   * @return the executing {@link LocalFragment} that this {@link Operator} is part of.
   */
  public LocalFragment getFragment() {
    if (execEnvVars == null) {
      return null;
    } else {
      return ((LocalFragmentResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER))
          .getFragment();
    }
  }

  /**
   * fragment id of this operator.
   */
  private Integer fragmentId;

  /**
   * @return fragment Id.
   */
  public int getFragmentId() {
    Objects.requireNonNull(fragmentId, "fragmentId");
    return fragmentId;
  }

  /**
   * @param fragmentId fragment Id.
   */
  public void setFragmentId(final int fragmentId) {
    this.fragmentId = fragmentId;
  }

  /**
   * @return the profiling modes.
   */
  @Nonnull
  protected Set<ProfilingMode> getProfilingMode() {
    // make sure hard coded test will pass
    if (execEnvVars == null) {
      return ImmutableSet.of();
    }
    if (profilingMode == null) {
      LocalFragmentResourceManager lfrm =
          (LocalFragmentResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER);
      if (lfrm == null) {
        return ImmutableSet.of();
      }
      LocalFragment fragment = lfrm.getFragment();
      if (fragment == null) {
        return ImmutableSet.of();
      }
      profilingMode = fragment.getLocalSubQuery().getProfilingMode();
    }
    return profilingMode;
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
   * Check if EOS is set.
   * 
   * This method is non-blocking.
   * 
   * @return if the Operator is at EOS (End of Stream)
   * 
   */
  public final boolean eos() {
    return eos;
  }

  /**
   * @return if the operator received an EOI (End of Iteration)
   */
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
   */
  protected void checkEOSAndEOI() {
    // this is the implementation for ordinary operators, e.g. join, project.
    // some operators have their own logics, e.g. LeafOperator, IDBController.
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
   */
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

    long startTime = -1;
    if (getProfilingMode().contains(ProfilingMode.QUERY)) {
      startTime = profilingLogger.getTime(this);
    }

    TupleBatch result = null;
    try {
      do {
        result = fetchNextReady();
        // XXX while or not while? For a single thread operator, while sounds more efficient
        // generally
      } while (result != null && result.numTuples() <= 0);
    } catch (RuntimeException | DbException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException(e);
    }
    if (getProfilingMode().contains(ProfilingMode.QUERY)) {
      int numberOfTupleReturned = -1;
      if (result != null) {
        numberOfTupleReturned = result.numTuples();
      }
      profilingLogger.recordEvent(this, numberOfTupleReturned, startTime);
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
   */
  private long numOutputTuples;

  /**
   * A simple statistic. The number of output TBs generated by this Operator.
   */
  private long numOutputTBs;

  /**
   * open the operator and do initializations.
   * 
   * @param execEnvVars the environment variables of the execution unit.
   * 
   * @throws DbException if any error occurs
   */
  public final void open(final Map<String, Object> execEnvVars) throws DbException {
    // open the children first
    if (open) {
      // XXX Do some error handling to multi-open?
      throw new DbException("Operator (opName=" + getOpName() + ") already open.");
    }
    if (execEnvVars == null) {
      this.execEnvVars = null;
    } else {
      this.execEnvVars = ImmutableMap.copyOf(execEnvVars);
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
    try {
      init(this.execEnvVars);
    } catch (DbException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new DbException(e);
    }
    open = true;

    /** @JORTIZ: When would this not be true?? */
    if (getLocalSubQuery() instanceof WorkerSubQuery) {
      worker = ((WorkerSubQuery) getLocalSubQuery()).getWorker();
    }

    if (getProfilingMode().size() > 0) {
      if (getLocalSubQuery() instanceof WorkerSubQuery) {
        profilingLogger = ((WorkerSubQuery) getLocalSubQuery()).getWorker().getProfilingLogger();
      }
    }
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
   */
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
   */
  protected abstract TupleBatch fetchNextReady() throws Exception;

  /**
   * Explicitly set EOS for this operator.
   * 
   * Operators should not be able to unset an already set EOS except reopen it.
   */
  protected final void setEOS() {
    if (eos()) {
      return;
    }
    eos = true;
  }

  /**
   * Attempt to produce the {@link Schema} of the tuples generated by this operator. This function must handle cases
   * like <code>null</code> children or arguments, and return <code>null</code> if there is not enough information to
   * produce the schema.
   * 
   * @return the {@link Schema} of the tuples generated by this operator, or <code>null</code> if the operator does not
   *         yet have enough information to generate the schema.
   */
  protected abstract Schema generateSchema();

  /**
   * @return return the Schema of the output tuples of this operator.
   * 
   */
  public final Schema getSchema() {
    if (schema == null) {
      schema = generateSchema();
    }
    return schema;
  }

  /**
   * This method is blocking.
   * 
   * @param children the Operators which are to be set as the children(child) of this operator.
   */
  public abstract void setChildren(Operator[] children);

  /**
   * Store if the children have meet EOI.
   */
  private boolean[] childrenEOI = null;

  /**
   * @return children EOI status.
   */
  protected final boolean[] getChildrenEOI() {
    if (childrenEOI == null) {
      // getChildren() == null indicates a leaf operator, which has its own checkEOSAndEOI()
      childrenEOI = new boolean[getChildren().length];
    }
    return childrenEOI;
  }

  /**
   * For use in leaf operators.
   */
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
   * @param opId the opId to set
   */
  public void setOpId(final int opId) {
    this.opId = opId;
  }

  /**
   * get op name.
   * 
   * @return op name
   */
  public String getOpName() {
    return opName;
  }

  /**
   * @return The id of the node (worker or master) that is running this operator.
   */
  protected int getNodeID() {
    return (Integer) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_NODE_ID);
  }

  /**
   * Get the unique operator id.
   * 
   * @return the op id
   */
  @Nullable
  public Integer getOpId() {
    return opId;
  }
}
