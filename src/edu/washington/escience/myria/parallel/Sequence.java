package edu.washington.escience.myria.parallel;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

/**
 * A meta-task that multiple {@link MetaTask} in sequence.
 */
public final class Sequence extends MetaTask {
  /** The tasks to run. */
  private final List<MetaTask> tasks;

  /**
   * Construct a {@link MetaTask} that runs the given tasks in sequence.
   * 
   * @param tasks the tasks to be run.
   */
  public Sequence(final List<MetaTask> tasks) {
    this.tasks = ImmutableList.copyOf(Objects.requireNonNull(tasks, "tasks"));
  }

  /**
   * @return the tasks
   */
  public List<MetaTask> getTasks() {
    return tasks;
  }

  @Override
  public void instantiate(final LinkedList<MetaTask> metaQ, final LinkedList<SubQuery> subQueryQ, final Server server) {
    MetaTask checkTask = metaQ.peekFirst();
    Verify.verify(checkTask == this, "this Fragment %s should be the first object on the queue, not %s!", this,
        checkTask);
    metaQ.removeFirst();
    metaQ.addAll(tasks);
  }
}
