package edu.washington.escience.myria.api.encoding.meta;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.parallel.meta.MetaTask;
import edu.washington.escience.myria.parallel.meta.Sequence;

public class SequenceEncoding extends MetaTaskEncoding {
  @Required
  public List<MetaTaskEncoding> tasks;

  @Override
  public MetaTask getTask() {
    ImmutableList.Builder<MetaTask> ret = ImmutableList.builder();
    for (MetaTaskEncoding t : tasks) {
      ret.add(t.getTask());
    }
    return new Sequence(ret.build());
  }

  @Override
  public void validateExtra() {
    Preconditions.checkArgument(tasks.size() > 0, "Sequence cannot be empty");
    int i = 0;
    for (MetaTaskEncoding m : tasks) {
      Preconditions.checkNotNull(m, "task %s/%s", i, tasks.size());
      m.validate();
      ++i;
    }
  }

  @Override
  public Set<Integer> getWorkers() {
    ImmutableSet.Builder<Integer> ret = ImmutableSet.builder();
    for (MetaTaskEncoding t : tasks) {
      ret.addAll(t.getWorkers());
    }
    return ret.build();
  }

}
