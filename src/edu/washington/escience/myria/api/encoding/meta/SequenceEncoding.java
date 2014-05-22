package edu.washington.escience.myria.api.encoding.meta;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.parallel.meta.MetaTask;
import edu.washington.escience.myria.parallel.meta.Sequence;

public class SequenceEncoding extends MetaTaskEncoding {
  @Required
  public List<MetaTaskEncoding> tasks;

  @Override
  MetaTask getTask() {
    ImmutableList.Builder<MetaTask> ret = ImmutableList.builder();
    for (MetaTaskEncoding t : tasks) {
      ret.add(t.getTask());
    }
    return new Sequence(ret.build());
  }
}
