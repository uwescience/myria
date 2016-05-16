package edu.washington.escience.myria.api.encoding.plan;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.parallel.JsonSubQuery;
import edu.washington.escience.myria.parallel.QueryPlan;

public class SubQueryEncoding extends SubPlanEncoding {
  @Required public List<PlanFragmentEncoding> fragments;

  @JsonCreator
  public SubQueryEncoding(@JsonProperty("fragments") final List<PlanFragmentEncoding> fragments) {
    this.fragments = fragments;
  }

  @Override
  public QueryPlan getPlan() {
    return new JsonSubQuery(fragments);
  }

  @Override
  public void validateExtra() {
    int i = 0;
    for (PlanFragmentEncoding f : fragments) {
      Preconditions.checkNotNull(f, "fragment %s of %s is null", i, fragments.size());
      f.validate();
      ++i;
    }
  }

  @Override
  public Set<Integer> getWorkers() {
    ImmutableSet.Builder<Integer> ret = ImmutableSet.builder();
    for (PlanFragmentEncoding f : fragments) {
      ret.addAll(f.workers);
    }
    return ret.build();
  }
}
