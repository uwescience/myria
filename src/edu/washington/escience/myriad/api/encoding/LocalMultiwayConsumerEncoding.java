package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myriad.parallel.Server;
import edu.washington.escience.myriad.util.MyriaUtils;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class LocalMultiwayConsumerEncoding extends AbstractConsumerEncoding<LocalMultiwayConsumer> {
  public Schema argSchema;
  public String argOperatorId;
  private static final List<String> requiredArguments = ImmutableList.of("argSchema", "argOperatorId");

  @Override
  public void connect(Operator operator, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

  @Override
  public LocalMultiwayConsumer construct(Server server) {
    return new LocalMultiwayConsumer(argSchema, MyriaUtils.getSingleElement(getRealOperatorIds()));
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected List<String> getOperatorIds() {
    return ImmutableList.of(argOperatorId);
  }

}