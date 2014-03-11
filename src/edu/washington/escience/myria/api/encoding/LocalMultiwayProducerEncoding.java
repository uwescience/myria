package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.LocalMultiwayProducer;
import edu.washington.escience.myria.parallel.Server;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class LocalMultiwayProducerEncoding extends AbstractProducerEncoding<LocalMultiwayProducer> {

  @Override
  public LocalMultiwayProducer construct(Server server) {
    List<ExchangePairID> ids = getRealOperatorIds();
    return new LocalMultiwayProducer(null, ids.toArray(new ExchangePairID[ids.size()]));
  }
}