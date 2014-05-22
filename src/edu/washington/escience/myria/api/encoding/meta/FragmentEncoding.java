package edu.washington.escience.myria.api.encoding.meta;

import java.util.List;

import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.parallel.meta.JsonFragment;
import edu.washington.escience.myria.parallel.meta.MetaTask;

public class FragmentEncoding extends MetaTaskEncoding {
  @Required
  public List<PlanFragmentEncoding> fragments;

  @Override
  MetaTask getTask() {
    return new JsonFragment(fragments);
  }
}
