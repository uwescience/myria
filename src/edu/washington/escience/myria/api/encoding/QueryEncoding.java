package edu.washington.escience.myria.api.encoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.Consumer;
import edu.washington.escience.myria.parallel.EOSController;
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

  @JsonIgnore
  public Map<Integer, SingleQueryPlanWithArgs> instantiate(final Server server) throws CatalogException {
    /* First, we need to know which workers run on each plan. */
    setupWorkersForFragments(server);
    /* Next, we need to know which pipes (operators) are produced and consumed on which workers. */
    setupWorkerNetworkOperators();

    HashMap<String, PlanFragmentEncoding> op2OwnerFragmentMapping = new HashMap<String, PlanFragmentEncoding>();
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> op : fragment.operators) {
        op2OwnerFragmentMapping.put(op.opName, fragment);
      }
    }

    Map<Integer, SingleQueryPlanWithArgs> plan = new HashMap<Integer, SingleQueryPlanWithArgs>();
    HashMap<PlanFragmentEncoding, RootOperator> instantiatedFragments =
        new HashMap<PlanFragmentEncoding, RootOperator>();
    HashMap<String, Operator> allOperators = new HashMap<String, Operator>();
    MutableLong fragmentID = new MutableLong();
    for (PlanFragmentEncoding fragment : fragments) {
      RootOperator op =
          instantiateFragment(fragment, fragmentID, server, instantiatedFragments, op2OwnerFragmentMapping,
              allOperators);
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
  @JsonIgnore
  private void setupWorkersForFragments(final Server server) throws CatalogException {
    for (PlanFragmentEncoding fragment : fragments) {
      if (fragment.workers != null && fragment.workers.size() > 0) {
        /* The workers are set in the plan. */
        continue;
      }

      /* The workers are *not* set in the plan. Let's find out what they are. */
      fragment.workers = new ArrayList<Integer>();

      /* If the plan has scans, it has to run on all of those workers. */
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof TableScanEncoding) {
          TableScanEncoding scan = ((TableScanEncoding) operator);
          List<Integer> scanWorkers = server.getWorkersForRelation(scan.relationKey, scan.storedRelationId);
          if (scanWorkers == null) {
            throw new MyriaApiException(Status.BAD_REQUEST, "Unable to find workers that store "
                + scan.relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
          }
          fragment.workers.addAll(scanWorkers);
        }
      }
      if (fragment.workers.size() > 0) {
        continue;
      }

      /* No scans found: See if there's a CollectConsumer. */
      for (OperatorEncoding<?> operator : fragment.operators) {
        /* If the fragment has a CollectConsumer, it has to run on a single worker. */
        if (operator instanceof CollectConsumerEncoding) {
          /* Just pick the first alive worker. */
          fragment.workers.add(server.getAliveWorkers().iterator().next());
          break;
        }
      }
      if (fragment.workers.size() > 0) {
        continue;
      }

      /* If not, just add all the alive workers in the cluster. */
      fragment.workers.addAll(server.getAliveWorkers());
    }
  }

  /**
   * Loop through all the operators in a plan fragment and connect them up.
   */
  @JsonIgnore
  private void setupWorkerNetworkOperators() {
    Map<String, Set<Integer>> producerWorkerMap = new HashMap<String, Set<Integer>>();
    Map<ExchangePairID, Set<Integer>> consumerWorkerMap = new HashMap<ExchangePairID, Set<Integer>>();
    Map<String, List<ExchangePairID>> producerOutputChannels = new HashMap<String, List<ExchangePairID>>();
    List<IDBControllerEncoding> idbInputs = new ArrayList<IDBControllerEncoding>();
    /* Pass 1: map strings to real operator IDs, also collect producers and consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof AbstractConsumerEncoding) {
          AbstractConsumerEncoding<?> consumer = (AbstractConsumerEncoding<?>) operator;
          String sourceProducerID = consumer.getOperatorId();
          List<ExchangePairID> sourceProducerOutputChannels = producerOutputChannels.get(sourceProducerID);
          if (sourceProducerOutputChannels == null) {
            // The producer is not yet met
            sourceProducerOutputChannels = new ArrayList<ExchangePairID>();
            producerOutputChannels.put(sourceProducerID, sourceProducerOutputChannels);
          }
          ExchangePairID channelID = ExchangePairID.newID();
          consumer.setRealOperatorIds(Arrays.asList(new ExchangePairID[] { channelID }));
          sourceProducerOutputChannels.add(channelID);
          consumerWorkerMap.put(channelID, ImmutableSet.<Integer> builder().addAll(fragment.workers).build());

        } else if (operator instanceof AbstractProducerEncoding) {
          AbstractProducerEncoding<?> producer = (AbstractProducerEncoding<?>) operator;
          List<ExchangePairID> sourceProducerOutputChannels = producerOutputChannels.get(producer.opName);
          if (sourceProducerOutputChannels == null) {
            sourceProducerOutputChannels = new ArrayList<ExchangePairID>();
            producerOutputChannels.put(producer.opName, sourceProducerOutputChannels);
          }
          producer.setRealOperatorIds(sourceProducerOutputChannels);
          producerWorkerMap.put(producer.opName, ImmutableSet.<Integer> builder().addAll(fragment.workers).build());
        } else if (operator instanceof IDBControllerEncoding) {
          IDBControllerEncoding idbInput = (IDBControllerEncoding) operator;
          idbInputs.add(idbInput);
          List<ExchangePairID> sourceProducerOutputChannels = producerOutputChannels.get(idbInput.opName);
          if (sourceProducerOutputChannels == null) {
            sourceProducerOutputChannels = new ArrayList<ExchangePairID>();
            producerOutputChannels.put(idbInput.opName, sourceProducerOutputChannels);
          }
          producerWorkerMap.put(idbInput.opName, new HashSet<Integer>(fragment.workers));
        }
      }
    }
    for (IDBControllerEncoding idbInput : idbInputs) {
      idbInput.setRealControllerOperatorID(producerOutputChannels.get(idbInput.opName).get(0));
    }
    /* Pass 2: Populate the right fields in producers and consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof ExchangeEncoding) {
          ExchangeEncoding<?> exchange = (ExchangeEncoding<?>) operator;
          ImmutableSet.Builder<Integer> workers = ImmutableSet.builder();
          for (ExchangePairID id : exchange.getRealOperatorIds()) {
            if (exchange instanceof AbstractConsumerEncoding) {
              try {
                workers.addAll(producerWorkerMap.get(((AbstractConsumerEncoding<?>) exchange).getOperatorId()));
              } catch (NullPointerException ee) {
                System.err.println("Consumer: " + ((AbstractConsumerEncoding<?>) exchange).opName);
                System.err.println("Producer: " + ((AbstractConsumerEncoding<?>) exchange).argOperatorId);
                System.err.println("producerWorkerMap: " + producerWorkerMap);
                throw ee;
              }
            } else if (exchange instanceof AbstractProducerEncoding) {
              workers.addAll(consumerWorkerMap.get(id));
            } else {
              throw new IllegalStateException("ExchangeEncoding " + operator.getClass().getSimpleName()
                  + " is not a Producer or Consumer encoding");
            }
          }
          exchange.setRealWorkerIds(workers.build());
        } else if (operator instanceof IDBControllerEncoding) {
          IDBControllerEncoding idbController = (IDBControllerEncoding) operator;
          idbController.realControllerWorkerId =
              MyriaUtils.getSingleElement(consumerWorkerMap.get(idbController.getRealControllerOperatorID()));
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
  @JsonIgnore
  private RootOperator instantiateFragment(final PlanFragmentEncoding planFragment, final MutableLong fragmentId,
      final Server server, final HashMap<PlanFragmentEncoding, RootOperator> instantiatedFragments,
      final Map<String, PlanFragmentEncoding> opOwnerFragment, final Map<String, Operator> allOperators) {
    long myFragmentID = fragmentId.longValue();
    fragmentId.increment();
    RootOperator instantiatedFragment = instantiatedFragments.get(planFragment);
    if (instantiatedFragment != null) {
      return instantiatedFragment;
    }

    RootOperator fragmentRoot = null;
    CollectConsumer oldRoot = null;
    Map<String, Operator> myOperators = new HashMap<String, Operator>();
    HashMap<String, AbstractConsumerEncoding<?>> nonIterativeConsumers =
        new HashMap<String, AbstractConsumerEncoding<?>>();
    HashSet<IDBControllerEncoding> idbs = new HashSet<IDBControllerEncoding>();
    /* Instantiate all the operators. */
    for (OperatorEncoding<?> encoding : planFragment.operators) {
      if (encoding instanceof IDBControllerEncoding) {
        idbs.add((IDBControllerEncoding) encoding);
      }
      if (encoding instanceof AbstractConsumerEncoding<?>) {
        nonIterativeConsumers.put(encoding.opName, (AbstractConsumerEncoding<?>) encoding);
      }

      Operator op = encoding.construct(server);
      /* helpful for debugging. */
      op.setOpName(encoding.opName);
      op.setFragmentId(myFragmentID);
      myOperators.put(encoding.opName, op);
      if (op instanceof RootOperator) {
        if (fragmentRoot != null) {
          throw new MyriaApiException(Status.BAD_REQUEST, "Multiple " + RootOperator.class.getSimpleName()
              + " detected in the fragment: " + fragmentRoot.getOpName() + ", and " + encoding.opName);
        }
        fragmentRoot = (RootOperator) op;
      }
      if (op instanceof CollectConsumer) {
        oldRoot = (CollectConsumer) op;
      }
    }
    allOperators.putAll(myOperators);

    for (IDBControllerEncoding idb : idbs) {
      nonIterativeConsumers.remove(idb.argIterationInput);
      nonIterativeConsumers.remove(idb.argEosControllerInput);
    }

    Set<PlanFragmentEncoding> dependantFragments = new HashSet<PlanFragmentEncoding>();
    for (AbstractConsumerEncoding<?> c : nonIterativeConsumers.values()) {
      dependantFragments.add(opOwnerFragment.get(c.argOperatorId));
    }

    for (PlanFragmentEncoding f : dependantFragments) {
      fragmentId.decrement();
      instantiateFragment(f, fragmentId, server, instantiatedFragments, opOwnerFragment, allOperators);
    }

    for (AbstractConsumerEncoding<?> c : nonIterativeConsumers.values()) {
      Consumer consumer = (Consumer) myOperators.get(c.opName);
      String producingOpName = c.argOperatorId;
      Operator producingOp = allOperators.get(producingOpName);
      if (producingOp instanceof IDBController) {
        consumer.setSchema(IDBController.EOI_REPORT_SCHEMA);
      } else {
        consumer.setSchema(producingOp.getSchema());
      }
    }

    /* Connect all the operators. */
    for (OperatorEncoding<?> encoding : planFragment.operators) {
      encoding.connect(myOperators.get(encoding.opName), myOperators);
    }

    for (IDBControllerEncoding idb : idbs) {
      IDBController idbOp = (IDBController) myOperators.get(idb.opName);
      Operator initialInput = idbOp.getChildren()[IDBController.CHILDREN_IDX_INITIAL_IDB_INPUT];
      Consumer iterativeInput = (Consumer) idbOp.getChildren()[IDBController.CHILDREN_IDX_ITERATION_INPUT];
      Consumer eosControllerInput = (Consumer) idbOp.getChildren()[IDBController.CHILDREN_IDX_EOS_CONTROLLER_INPUT];
      iterativeInput.setSchema(initialInput.getSchema());
      eosControllerInput.setSchema(EOSController.EOS_REPORT_SCHEMA);
    }
    /* Return the root. */
    if (fragmentRoot == null && oldRoot != null) {
      /* Old query plan, add a SinkRoot to the top. */
      LOGGER.info("Adding a SinkRoot to the top of an old query plan.");
      fragmentRoot = new SinkRoot(oldRoot);
    }

    if (fragmentRoot == null) {
      throw new MyriaApiException(Status.BAD_REQUEST, "No " + RootOperator.class.getSimpleName()
          + " detected in the fragment.");
    }

    instantiatedFragments.put(planFragment, fragmentRoot);
    return fragmentRoot;
  }
}