package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
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

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argChild);
      Preconditions.checkNotNull(argOperatorIds);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_child, arg_operator_ids");
    }
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
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }
}