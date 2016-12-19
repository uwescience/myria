package edu.washington.escience.myria.api.encoding;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.util.MyriaUtils;

/** A JSON-able wrapper for the expected wire message for a new dataset. */
public class LocalMultiwayConsumerEncoding extends AbstractConsumerEncoding<Consumer> {

  @Override
  public Consumer construct(ConstructArgs args) {
    return new Consumer(
        null,
        MyriaUtils.getSingleElement(getRealOperatorIds()),
        ImmutableSet.of(IPCConnectionPool.SELF_IPC_ID));
  }
}
