package edu.washington.escience.myria.api.encoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.EOSController;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.util.MyriaUtils;

public class QueryConstruct {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QueryEncoding.class);

  public static Map<Integer, SingleQueryPlanWithArgs> instantiate(List<PlanFragmentEncoding> fragments,
      final Server server, FTMODE ftMode, boolean profilingMode) throws CatalogException {
    /* First, we need to know which workers run on each plan. */
    setupWorkersForFragments(fragments, server);
    /* Next, we need to know which pipes (operators) are produced and consumed on which workers. */
    setupWorkerNetworkOperators(fragments);

    HashMap<Integer, PlanFragmentEncoding> op2OwnerFragmentMapping = new HashMap<Integer, PlanFragmentEncoding>();
    int idx = 0;
    for (PlanFragmentEncoding fragment : fragments) {
      fragment.setFragmentIndex(idx++);
      for (OperatorEncoding<?> op : fragment.operators) {
        op2OwnerFragmentMapping.put(op.opId, fragment);
      }
    }

    Map<Integer, SingleQueryPlanWithArgs> plan = new HashMap<Integer, SingleQueryPlanWithArgs>();
    HashMap<PlanFragmentEncoding, RootOperator> instantiatedFragments =
        new HashMap<PlanFragmentEncoding, RootOperator>();
    HashMap<Integer, Operator> allOperators = new HashMap<Integer, Operator>();
    for (PlanFragmentEncoding fragment : fragments) {
      RootOperator op =
          instantiateFragment(fragment, server, instantiatedFragments, op2OwnerFragmentMapping, allOperators);
      for (Integer worker : fragment.workers) {
        SingleQueryPlanWithArgs workerPlan = plan.get(worker);
        if (workerPlan == null) {
          workerPlan = new SingleQueryPlanWithArgs();
          workerPlan.setFTMode(ftMode);
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
  private static void setupWorkersForFragments(List<PlanFragmentEncoding> fragments, final Server server)
      throws CatalogException {
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
          if (fragment.workers.size() == 0) {
            fragment.workers.addAll(scanWorkers);
          } else {
            /*
             * If the fragment already has workers, it scans multiple relations. They better use the exact same set of
             * workers.
             */
            if (fragment.workers.size() != scanWorkers.size() || !fragment.workers.containsAll(scanWorkers)) {
              throw new MyriaApiException(Status.BAD_REQUEST,
                  "All tables scanned within a fragment must use the exact same set of workers. Caught at TableScan("
                      + scan.relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + ")");
            }
          }
        }
      }
      if (fragment.workers.size() > 0) {
        continue;
      }

      /* No scans found: See if there's a CollectConsumer. */
      for (OperatorEncoding<?> operator : fragment.operators) {
        /* If the fragment has a CollectConsumer, it has to run on a single worker. */
        if (operator instanceof CollectConsumerEncoding || operator instanceof SingletonEncoding) {
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
  private static void setupWorkerNetworkOperators(List<PlanFragmentEncoding> fragments) {
    Map<Integer, Set<Integer>> producerWorkerMap = new HashMap<Integer, Set<Integer>>();
    Map<ExchangePairID, Set<Integer>> consumerWorkerMap = new HashMap<ExchangePairID, Set<Integer>>();
    Map<Integer, List<ExchangePairID>> producerOutputChannels = new HashMap<Integer, List<ExchangePairID>>();
    List<IDBControllerEncoding> idbInputs = new ArrayList<IDBControllerEncoding>();
    /* Pass 1: map strings to real operator IDs, also collect producers and consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof AbstractConsumerEncoding) {
          AbstractConsumerEncoding<?> consumer = (AbstractConsumerEncoding<?>) operator;
          Integer sourceProducerID = consumer.getOperatorId();
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
          List<ExchangePairID> sourceProducerOutputChannels = producerOutputChannels.get(producer.opId);
          if (sourceProducerOutputChannels == null) {
            sourceProducerOutputChannels = new ArrayList<ExchangePairID>();
            producerOutputChannels.put(producer.opId, sourceProducerOutputChannels);
          }
          producer.setRealOperatorIds(sourceProducerOutputChannels);
          producerWorkerMap.put(producer.opId, ImmutableSet.<Integer> builder().addAll(fragment.workers).build());
        } else if (operator instanceof IDBControllerEncoding) {
          IDBControllerEncoding idbInput = (IDBControllerEncoding) operator;
          idbInputs.add(idbInput);
          List<ExchangePairID> sourceProducerOutputChannels = producerOutputChannels.get(idbInput.opId);
          if (sourceProducerOutputChannels == null) {
            sourceProducerOutputChannels = new ArrayList<ExchangePairID>();
            producerOutputChannels.put(idbInput.opId, sourceProducerOutputChannels);
          }
          producerWorkerMap.put(idbInput.opId, new HashSet<Integer>(fragment.workers));
        }
      }
    }
    for (IDBControllerEncoding idbInput : idbInputs) {
      idbInput.setRealControllerOperatorID(producerOutputChannels.get(idbInput.opId).get(0));
    }
    /* Pass 2: Populate the right fields in producers and consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof ExchangeEncoding) {
          ExchangeEncoding exchange = (ExchangeEncoding) operator;
          ImmutableSet.Builder<Integer> workers = ImmutableSet.builder();
          for (ExchangePairID id : exchange.getRealOperatorIds()) {
            if (exchange instanceof AbstractConsumerEncoding) {
              try {
                workers.addAll(producerWorkerMap.get(((AbstractConsumerEncoding<?>) exchange).getOperatorId()));
              } catch (NullPointerException ee) {
                System.err.println("Consumer: " + ((AbstractConsumerEncoding<?>) exchange).opId);
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
  private static RootOperator instantiateFragment(final PlanFragmentEncoding planFragment, final Server server,
      final HashMap<PlanFragmentEncoding, RootOperator> instantiatedFragments,
      final Map<Integer, PlanFragmentEncoding> opOwnerFragment, final Map<Integer, Operator> allOperators) {
    RootOperator instantiatedFragment = instantiatedFragments.get(planFragment);
    if (instantiatedFragment != null) {
      return instantiatedFragment;
    }

    RootOperator fragmentRoot = null;
    CollectConsumer oldRoot = null;
    Map<Integer, Operator> myOperators = new HashMap<Integer, Operator>();
    HashMap<Integer, AbstractConsumerEncoding<?>> nonIterativeConsumers =
        new HashMap<Integer, AbstractConsumerEncoding<?>>();
    HashSet<IDBControllerEncoding> idbs = new HashSet<IDBControllerEncoding>();
    /* Instantiate all the operators. */
    for (OperatorEncoding<?> encoding : planFragment.operators) {
      if (encoding instanceof IDBControllerEncoding) {
        idbs.add((IDBControllerEncoding) encoding);
      }
      if (encoding instanceof AbstractConsumerEncoding<?>) {
        nonIterativeConsumers.put(encoding.opId, (AbstractConsumerEncoding<?>) encoding);
      }

      Operator op = encoding.construct(server);
      /* helpful for debugging. */
      op.setOpName("Operator" + String.valueOf(encoding.opId));
      op.setFragmentId(planFragment.fragmentIndex);
      myOperators.put(encoding.opId, op);
      if (op instanceof RootOperator) {
        if (fragmentRoot != null) {
          throw new MyriaApiException(Status.BAD_REQUEST, "Multiple " + RootOperator.class.getSimpleName()
              + " detected in the fragment: " + fragmentRoot.getOpName() + ", and " + encoding.opId);
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
      instantiateFragment(f, server, instantiatedFragments, opOwnerFragment, allOperators);
    }

    for (AbstractConsumerEncoding<?> c : nonIterativeConsumers.values()) {
      Consumer consumer = (Consumer) myOperators.get(c.opId);
      Integer producingOpName = c.argOperatorId;
      Operator producingOp = allOperators.get(producingOpName);
      if (producingOp instanceof IDBController) {
        consumer.setSchema(IDBController.EOI_REPORT_SCHEMA);
      } else {
        consumer.setSchema(producingOp.getSchema());
      }
    }

    /* Connect all the operators. */
    for (OperatorEncoding<?> encoding : planFragment.operators) {
      encoding.connect(myOperators.get(encoding.opId), myOperators);
    }

    for (IDBControllerEncoding idb : idbs) {
      IDBController idbOp = (IDBController) myOperators.get(idb.opId);
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
