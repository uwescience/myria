package edu.washington.escience.myria.api.encoding.meta;

import java.util.List;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.parallel.meta.JsonFragment;
import edu.washington.escience.myria.parallel.meta.MetaTask;

public class FragmentEncoding extends MetaTaskEncoding {
  @Required
  public List<PlanFragmentEncoding> fragments;

  @Override
  public MetaTask getTask() {
    return new JsonFragment(fragments);
  }

  @Override
  public void validateExtra() {
    int i = 0;
    for (PlanFragmentEncoding f : fragments) {
      Preconditions.checkNotNull(f, "fragment %s/%s", i, fragments.size());
      f.validate();
      ++i;
    }
  }
}
