package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalMultiwayProducer;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class LocalMultiwayProducerEncoding extends OperatorEncoding<LocalMultiwayProducer> {
  public String argChild;
  public int[] argOperatorIds;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argOperatorIds");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public LocalMultiwayProducer construct() {
    ExchangePairID[] tmp = new ExchangePairID[argOperatorIds.length];
    for (int i = 0; i < argOperatorIds.length; ++i) {
      tmp[i] = ExchangePairID.fromExisting(argOperatorIds[i]);
    }
    return new LocalMultiwayProducer(null, tmp);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}