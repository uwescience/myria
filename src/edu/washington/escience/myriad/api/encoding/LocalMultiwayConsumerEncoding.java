package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalMultiwayConsumer;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class LocalMultiwayConsumerEncoding extends OperatorEncoding<LocalMultiwayConsumer> {
  public Schema argSchema;
  public int[] argWorkerIds;
  public Integer argOperatorId;
  private static final List<String> requiredArguments = ImmutableList.of("argSchema", "argWorkerIds", "argOperatorId");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

  @Override
  public LocalMultiwayConsumer construct() {
    return new LocalMultiwayConsumer(argSchema, ExchangePairID.fromExisting(argOperatorId));
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}