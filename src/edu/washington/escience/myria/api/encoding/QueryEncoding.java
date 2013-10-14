package edu.washington.escience.myria.api.encoding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A JSON-able wrapper for the expected wire message for a query.
 * 
 */
@JsonIgnoreProperties({ "expected_result" })
public class QueryEncoding extends MyriaApiEncoding {
  /** The raw Datalog. */
  public String rawDatalog;
  /** The logical relation algebra plan. */
  public String logicalRa;
  /** The query plan encoding. */
  public List<PlanFragmentEncoding> fragments;
  /** Set whether this query needs profiling. (default is false) */
  public boolean profilingMode = false;
  /** The expected number of results (for testing). */
  public Long expectedResultSize;
  /** The list of required fields. */
  private static List<String> requiredFields = ImmutableList.of("rawDatalog", "logicalRa", "fragments");
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QueryEncoding.class);
  /** The fault-tolerance mode used in this query, default: none. */
  public String ftMode = "none";

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

  @Override
  protected void validateExtra() throws MyriaApiException {
    for (final PlanFragmentEncoding fragment : fragments) {
      fragment.validate();
    }
  }

  public Map<Integer, SingleQueryPlanWithArgs> instantiate(final Server server) throws CatalogException {
    /* First, we need to know which workers run on each plan. */
    setupWorkersForFragments(server);
    /* Next, we need to know which pipes (operators) are produced and consumed on which workers. */
    setupWorkerNetworkOperators();
    Map<Integer, SingleQueryPlanWithArgs> plan = new HashMap<Integer, SingleQueryPlanWithArgs>();
    for (PlanFragmentEncoding fragment : fragments) {
      RootOperator op = instantiatePlanFragment(fragment.operators, fragments.indexOf(fragment), server);
      for (Integer worker : fragment.workers) {
        SingleQueryPlanWithArgs workerPlan = plan.get(worker);
        if (workerPlan == null) {
          workerPlan = new SingleQueryPlanWithArgs();
          workerPlan.setFTMode(FTMODE.valueOf(ftMode));
          workerPlan.setProfilingMode(profilingMode);
          plan.put(worker, workerPlan);
        }
        workerPlan.addRootOp(op);
      }
    }
    return plan;
  }

  /**
   * Figures out which workers are needed for every fragment.
   * 
   * @throws CatalogException if there is an error in the Catalog.
   */
  private void setupWorkersForFragments(final Server server) throws CatalogException {
    for (PlanFragmentEncoding fragment : fragments) {
      if (fragment.workers != null && fragment.workers.size() > 0) {
        /* The workers are set in the plan. */
        continue;
      }

      /* The workers are *not* set in the plan. Let's find out what they are. */
      List<Integer> workers = new ArrayList<Integer>();
      fragment.workers = workers;

      /* If the plan has scans, it has to run on all of those workers. */
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof TableScanEncoding) {
          TableScanEncoding scan = ((TableScanEncoding) operator);
          List<Integer> scanWorkers = server.getWorkersForRelation(scan.relationKey, scan.storedRelationId);
          if (scanWorkers == null) {
            throw new MyriaApiException(Status.BAD_REQUEST, "Unable to find workers that store "
                + scan.relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
          }
          workers.addAll(scanWorkers);
        }
      }
      if (workers.size() > 0) {
        continue;
      }

      /* No scans found: See if there's a CollectConsumer. */
      for (OperatorEncoding<?> operator : fragment.operators) {
        /* If the fragment has a CollectConsumer, it has to run on a single worker. */
        if (operator instanceof CollectConsumerEncoding) {
          /* Just pick the first alive worker. */
          workers.add(server.getAliveWorkers().iterator().next());
          break;
        }
      }
      if (workers.size() > 0) {
        continue;
      }

      /* If not, just add all the alive workers in the cluster. */
      workers.addAll(server.getAliveWorkers());
    }
  }

  /**
   * Loop through all the operators in a plan fragment and connect them up.
   */
  private void setupWorkerNetworkOperators() {
    Map<String, ExchangePairID> operatorIdMap = new HashMap<String, ExchangePairID>();
    Map<ExchangePairID, List<Integer>> producerMap = new HashMap<ExchangePairID, List<Integer>>();
    Map<ExchangePairID, List<Integer>> consumerMap = new HashMap<ExchangePairID, List<Integer>>();
    /* Pass 1: map strings to real operator IDs, also collect producers and consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof ExchangeEncoding) {
          ExchangeEncoding<?> exchange = (ExchangeEncoding<?>) operator;
          List<String> argIds = exchange.getOperatorIds();
          List<ExchangePairID> ids = new ArrayList<ExchangePairID>(argIds.size());
          for (String argId : argIds) {
            ExchangePairID id = operatorIdMap.get(argId);
            if (id == null) {
              id = ExchangePairID.newID();
              operatorIdMap.put(argId, id);
              producerMap.put(id, new ArrayList<Integer>());
              consumerMap.put(id, new ArrayList<Integer>());
            }
            if (exchange instanceof AbstractConsumerEncoding) {
              consumerMap.get(id).addAll(fragment.workers);
            } else if (exchange instanceof AbstractProducerEncoding) {
              producerMap.get(id).addAll(fragment.workers);
            } else {
              throw new IllegalStateException("ExchangeEncoding " + operator.getClass().getSimpleName()
                  + " is not a Producer or Consumer encoding");
            }
            ids.add(id);
          }
          exchange.setRealOperatorIds(ids);
        } else if (operator instanceof IDBControllerEncoding) {
          IDBControllerEncoding idbController = (IDBControllerEncoding) operator;
          ExchangePairID id = operatorIdMap.get(idbController.argControllerOperatorId);
          if (id == null) {
            id = ExchangePairID.newID();
            operatorIdMap.put(idbController.argControllerOperatorId, id);
            producerMap.put(id, new ArrayList<Integer>());
            consumerMap.put(id, new ArrayList<Integer>());
          }
          producerMap.get(id).addAll(fragment.workers);
          idbController.realControllerOperatorId = id;
        }
      }
    }
    /* Pass 2: Populate the right fields in producers and consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof ExchangeEncoding) {
          ExchangeEncoding<?> exchange = (ExchangeEncoding<?>) operator;
          ImmutableSet.Builder<Integer> workers = ImmutableSet.builder();
          for (ExchangePairID id : exchange.getRealOperatorIds()) {
            if (exchange instanceof AbstractConsumerEncoding) {
              workers.addAll(producerMap.get(id));
            } else if (exchange instanceof AbstractProducerEncoding) {
              workers.addAll(consumerMap.get(id));
            } else {
              throw new IllegalStateException("ExchangeEncoding " + operator.getClass().getSimpleName()
                  + " is not a Producer or Consumer encoding");
            }
          }
          exchange.setRealWorkerIds(workers.build());
        } else if (operator instanceof IDBControllerEncoding) {
          IDBControllerEncoding idbController = (IDBControllerEncoding) operator;
          idbController.realControllerWorkerId =
              MyriaUtils.getSingleElement(consumerMap.get(idbController.realControllerOperatorId));
        }
      }
    }
  }

  /**
   * Given an encoding of a plan fragment, i.e., a connected list of operators, instantiate the actual plan fragment.
   * This includes instantiating the operators and connecting them together. The constraint on the plan fragments is
   * that the last operator in the fragment must be the RootOperator. There is a special exception for older plans in
   * which a CollectConsumer will automatically have a SinkRoot appended to it.
   * 
   * @param planFragment the encoded plan fragment.
   * @return the actual plan fragment.
   */
  private static RootOperator instantiatePlanFragment(final List<OperatorEncoding<?>> planFragment,
      final long fragmentId, final Server server) {
    Map<String, Operator> operators = new HashMap<String, Operator>();

    /* Instantiate all the operators. */
    for (OperatorEncoding<?> encoding : planFragment) {
      Operator op = encoding.construct(server);
      /* helpful for debugging. */
      op.setOpName(encoding.opName);
      op.setFragmentId(fragmentId);
      operators.put(encoding.opName, op);
    }
    /* Connect all the operators. */
    for (OperatorEncoding<?> encoding : planFragment) {
      encoding.connect(operators.get(encoding.opName), (operators));
    }
    /* Return the first one. */
    Operator ret = operators.get(planFragment.get(planFragment.size() - 1).opName);
    if (ret instanceof RootOperator) {
      return (RootOperator) ret;
    } else if (ret instanceof CollectConsumer) {
      /* Old query plan, add a SinkRoot to the top. */
      LOGGER.info("Adding a SinkRoot to the top of an old query plan.");
      SinkRoot sinkRoot = new SinkRoot(ret);
      return sinkRoot;
    } else {
      throw new MyriaApiException(Status.BAD_REQUEST,
          "The last operator in a plan fragment must be a RootOperator, not " + ret.getClass());
    }
  }
}