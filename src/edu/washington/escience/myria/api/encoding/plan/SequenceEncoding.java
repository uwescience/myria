package edu.washington.escience.myria.api.encoding.plan;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.parallel.QueryPlan;
import edu.washington.escience.myria.parallel.Sequence;

public class SequenceEncoding extends SubPlanEncoding {
  @Required public List<SubPlanEncoding> plans;

  @Override
  public QueryPlan getPlan() {
    ImmutableList.Builder<QueryPlan> ret = ImmutableList.builder();
    for (SubPlanEncoding p : plans) {
      ret.add(p.getPlan());
    }
    return new Sequence(ret.build());
  }

  @Override
  public void validateExtra() {
    Preconditions.checkArgument(plans.size() > 0, "Sequence cannot be empty");
    int i = 0;
    for (SubPlanEncoding p : plans) {
      Preconditions.checkNotNull(p, "plan %s/%s is null", i, plans.size());
      p.validate();
      ++i;
    }
  }

  @Override
  public Set<Integer> getWorkers() {
    ImmutableSet.Builder<Integer> ret = ImmutableSet.builder();
    for (SubPlanEncoding p : plans) {
      ret.addAll(p.getWorkers());
    }
    return ret.build();
  }
}
