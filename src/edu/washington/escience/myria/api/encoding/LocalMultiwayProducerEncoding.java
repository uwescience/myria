package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.LocalMultiwayProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;

/** JSON wrapper for LocalMultiwayProducer. */
public class LocalMultiwayProducerEncoding extends AbstractProducerEncoding<LocalMultiwayProducer> {

  @Override
  public LocalMultiwayProducer construct(ConstructArgs args) {
    return new LocalMultiwayProducer(null, getRealOperatorIds().toArray(new ExchangePairID[] {}));
  }
}
