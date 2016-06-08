package edu.washington.escience.myria.api.encoding.plan;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.parallel.DoWhile;
import edu.washington.escience.myria.parallel.QueryPlan;

public class DoWhileEncoding extends SubPlanEncoding {
  @Required public List<SubPlanEncoding> body;
  @Required public String condition;

  @Override
  public QueryPlan getPlan() {
    ImmutableList.Builder<QueryPlan> ret = ImmutableList.builder();
    for (SubPlanEncoding p : body) {
      ret.add(p.getPlan());
    }
    return new DoWhile(ret.build(), condition);
  }

  @Override
  public void validateExtra() {
    Preconditions.checkArgument(body.size() > 0, "DoWhile cannot be empty");
    int i = 0;
    for (SubPlanEncoding p : body) {
      Preconditions.checkNotNull(p, "body plan %s/%s is null", i, body.size());
      p.validate();
      ++i;
    }
  }

  @Override
  public Set<Integer> getWorkers() {
    ImmutableSet.Builder<Integer> ret = ImmutableSet.builder();
    for (SubPlanEncoding p : body) {
      ret.addAll(p.getWorkers());
    }
    return ret.build();
  }
}
