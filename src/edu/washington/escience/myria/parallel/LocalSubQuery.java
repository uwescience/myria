package edu.washington.escience.myria.parallel;

import java.util.Set;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;

/**
 * A {@link LocalSubQuery} is the instantiation of the part of a distributed subquery that executes at one node. It
 * typically contains one or more {@link LocalFragment}s that represent the individual subquery fragments that make up
 * the local component of the subquery.
 */
public abstract class LocalSubQuery implements Comparable<LocalSubQuery> {

  /**
   * The profiling mode.
   */
  private final Set<ProfilingMode> profilingMode;

  /**
   * The fault tolerance mode.
   */
  private final FTMode ftMode;

  /**
   * Priority, currently not used.
   */
  private static final int PRIORITY = 0;

  /**
   * get the ftMode.
   *
   * @return the ft mode.
   */
  public final FTMode getFTMode() {
    return ftMode;
  }

  /**
   * The ID of this subquery.
   */
  private final SubQueryId subQueryId;

  /**
   * Instantiate a new {@link LocalSubQuery} with the specified fault tolerance and profiling modes.
   *
   * @param subQueryId the id of this subquery.
   * @param ftMode the fault-tolerance mode of this subquery.
   * @param profilingMode the profiling mode of this subquery.
   */
  public LocalSubQuery(
      final SubQueryId subQueryId,
      final FTMode ftMode,
      @Nonnull final Set<ProfilingMode> profilingMode) {
    this.subQueryId = subQueryId;
    this.ftMode = ftMode;
    this.profilingMode = profilingMode;
  }

  /**
   * @return the profiling mode.
   */
  @Nonnull
  public final Set<ProfilingMode> getProfilingMode() {
    return profilingMode;
  }

  /**
   * @return The (global) ID for this subquery.
   */
  public final SubQueryId getSubQueryId() {
    return subQueryId;
  }

  /**
   * @return the priority of this subquery.
   */
  final int getPriority() {
    /*
     * TODO from @dhalperi, why is any of this here? It seems to never be used anywhere, and the comments in
     * WorkerSubQuery.java make me think this is an accurate assessment.
     */
    return PRIORITY;
  }

  /**
   * Start execution.
   */
  abstract void startExecution();

  /**
   * Prepare to execute, reserve resources, allocate data structures to be used in execution, etc.
   */
  abstract void init();

  /**
   * Kill this subquery.
   */
  abstract void kill();

  /**
   * Statistics of this {@link LocalSubQuery}.
   */
  private final ExecutionStatistics queryStatistics = new ExecutionStatistics();

  /**
   * @return the execution statistics for this subquery.
   */
  final ExecutionStatistics getExecutionStatistics() {
    return queryStatistics;
  }

  /**
   * @return the set of workers that are currently dead.
   */
  public abstract Set<Integer> getMissingWorkers();

  /**
   * @return the future for the query's execution.
   */
  abstract LocalSubQueryFuture getExecutionFuture();

  /**
   * enable/disable output channels of the root(producer) of the {@link LocalFragment}.
   *
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   */
  final void updateProducerChannels(final int workerId, final boolean enable) {
    for (LocalFragment fragment : getFragments()) {
      fragment.updateProducerChannels(workerId, enable);
    }
  }

  /**
   * when a REMOVE_WORKER message is received, give all the {@link LocalFragment}s of this {@link LocalSubQuery} another
   * chance to decide if they are ready to generate EOS/EOI.
   */
  final void triggerFragmentEosEoiChecks() {
    for (LocalFragment fragment : getFragments()) {
      fragment.notifyNewInput();
    }
  }

  @Override
  public final int compareTo(final LocalSubQuery o) {
    if (o == null) {
      return -1;
    }
    return getPriority() - o.getPriority();
  }

  /**
   * Returns the local fragments of this subquery.
   *
   * @return the local fragments of this subquery.
   */
  abstract Set<LocalFragment> getFragments();
}
