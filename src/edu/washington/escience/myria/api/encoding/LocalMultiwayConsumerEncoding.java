package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.LocalMultiwayConsumer;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 *
 */
public class LocalMultiwayConsumerEncoding extends AbstractConsumerEncoding<LocalMultiwayConsumer> {

  @Override
  public LocalMultiwayConsumer construct(ConstructArgs args) {
    return new LocalMultiwayConsumer(null, MyriaUtils.getSingleElement(getRealOperatorIds()));
  }
}
