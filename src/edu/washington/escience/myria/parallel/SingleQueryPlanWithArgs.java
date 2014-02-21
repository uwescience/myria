package edu.washington.escience.myria.parallel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.operator.RootOperator;

/**
 * Class that contains a list of RootOperators with parameters associated with the query, for example, FT mode. We may
 * have more parameters once Myria is launched as a service, in that case a query would have parameters such as resource
 * limits.
 */
public class SingleQueryPlanWithArgs implements Serializable {

  /** Serialization. */
  private static final long serialVersionUID = 1L;

  /** The list of RootOperators. */
  private final List<RootOperator> rootOps;

  /** FT mode, default: none. */
  private FTMODE ftMode = FTMODE.valueOf("none");

  /**
   * profilingMode,default:false.
   */
  private boolean profilingMode = false;

  /** Constructor. */
  public SingleQueryPlanWithArgs() {
    rootOps = new ArrayList<RootOperator>();
  }

  /**
   * Constructor.
   * 
   * @param op a root operator.
   * */
  public SingleQueryPlanWithArgs(final RootOperator op) {
    this();
    addRootOp(op);
  }

  /**
   * Constructor.
   * 
   * @param ops a list of root operators.
   * */
  public SingleQueryPlanWithArgs(final RootOperator[] ops) {
    this();
    addRootOp(ops);
  }

  /**
   * Constructor.
   * 
   * @param ops a list of root operators.
   * */
  public SingleQueryPlanWithArgs(final List<RootOperator> ops) {
    this();
    addRootOp(ops);
  }

  /**
   * Return RootOperators.
   * 
   * @return the rootOps.
   * */
  public List<RootOperator> getRootOps() {
    return rootOps;
  }

  /**
   * Add a RootOperator.
   * 
   * @param op the operator.
   * */
  public void addRootOp(final RootOperator op) {
    rootOps.add(op);
  }

  /**
   * Add a list of RootOperator.
   * 
   * @param ops operators.
   * */
  public void addRootOp(final RootOperator[] ops) {
    for (RootOperator op : ops) {
      addRootOp(op);
    }
  }

  /**
   * Add a list of RootOperator.
   * 
   * @param ops operators.
   * */
  public void addRootOp(final List<RootOperator> ops) {
    for (RootOperator op : ops) {
      addRootOp(op);
    }
  }

  /**
   * Set FT mode.
   * 
   * @param ftMode the mode.
   * */
  public void setFTMode(final FTMODE ftMode) {
    this.ftMode = ftMode;
  }

  /**
   * Return FT mode.
   * 
   * @return the ft mode.
   * */
  public FTMODE getFTMode() {
    return ftMode;
  }

  /**
   * @return the profiling mode.
   */
  public boolean isProfilingMode() {
    return profilingMode;
  }

  /**
   * 
   * Set profiling mode.
   * 
   * @param profilingMode the profiling mode.
   */
  public void setProfilingMode(final boolean profilingMode) {
    this.profilingMode = profilingMode;
  }
}
