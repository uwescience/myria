package edu.washington.escience.myria.parallel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.MyriaConstants.PROFILING_MODE;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.operator.DbReader;
import edu.washington.escience.myria.operator.DbWriter;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Class that contains a list of RootOperators with parameters associated with the query, for example, FT mode. We may
 * have more parameters once Myria is launched as a service, in that case a query would have parameters such as resource
 * limits.
 */
public class SubQueryPlan implements Serializable, DbReader, DbWriter {

  /** Serialization. */
  private static final long serialVersionUID = 1L;

  /** The list of RootOperators. */
  private final List<RootOperator> rootOps;

  /** FT mode, default: none. */
  private FTMODE ftMode = FTMODE.NONE;

  /** The relations that are written, along with their schemas. */
  private final Map<RelationKey, RelationWriteMetadata> writeSet;
  /** The relations that are read. */
  private final Set<RelationKey> readSet;

  /**
   * profilingMode,default:none.
   */
  private PROFILING_MODE profilingMode = PROFILING_MODE.NONE;

  /** Constructor. */
  public SubQueryPlan() {
    rootOps = new ArrayList<RootOperator>();
    writeSet = Maps.newHashMap();
    readSet = Sets.newHashSet();
  }

  /**
   * Constructor.
   * 
   * @param op a root operator.
   * */
  public SubQueryPlan(final RootOperator op) {
    this();
    addRootOp(op);
  }

  /**
   * Constructor.
   * 
   * @param ops a list of root operators.
   * */
  public SubQueryPlan(final RootOperator[] ops) {
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
    updateReadWriteSets(op);
  }

  /**
   * A helper to walk various operators and compute what relations they read and write. This is for understanding query
   * contention.
   * 
   * @param op a single operator, which will be recursively traversed
   */
  private void updateReadWriteSets(final Operator op) {
    if (op instanceof DbWriter) {
      MyriaUtils.putNewVerifyOld(((DbWriter) op).writeSet(), writeSet);
    } else if (op instanceof DbReader) {
      readSet.addAll(((DbReader) op).readSet());
    }

    for (Operator child : op.getChildren()) {
      updateReadWriteSets(child);
    }
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
  public PROFILING_MODE getProfilingMode() {
    return profilingMode;
  }

  /**
   * Set profiling mode.
   * 
   * @param profilingMode the profiling mode.
   */
  public void setProfilingMode(final PROFILING_MODE profilingMode) {
    this.profilingMode = profilingMode;
  }

  @Override
  public Map<RelationKey, RelationWriteMetadata> writeSet() {
    return ImmutableMap.copyOf(writeSet);
  }

  @Override
  public Set<RelationKey> readSet() {
    return ImmutableSet.copyOf(readSet);
  }
}
