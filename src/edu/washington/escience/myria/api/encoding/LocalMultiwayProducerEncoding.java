package edu.washington.escience.myria.api.encoding;

import java.util.List;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.LocalMultiwayProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class LocalMultiwayProducerEncoding extends AbstractProducerEncoding<LocalMultiwayProducer> {

  @Override
  public LocalMultiwayProducer construct(@Nonnull ConstructArgs args) {
    List<ExchangePairID> ids = getRealOperatorIds();
    return new LocalMultiwayProducer(null, ids.toArray(new ExchangePairID[ids.size()]));
  }
}