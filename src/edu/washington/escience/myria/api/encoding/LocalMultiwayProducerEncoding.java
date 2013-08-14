package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.LocalMultiwayProducer;
import edu.washington.escience.myria.parallel.Server;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class LocalMultiwayProducerEncoding extends AbstractProducerEncoding<LocalMultiwayProducer> {
  public String argChild;
  public List<String> argOperatorIds;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argOperatorIds");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public LocalMultiwayProducer construct(Server server) {
    List<ExchangePairID> ids = getRealOperatorIds();
    return new LocalMultiwayProducer(null, ids.toArray(new ExchangePairID[ids.size()]));
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected List<String> getOperatorIds() {
    return argOperatorIds;
  }
}