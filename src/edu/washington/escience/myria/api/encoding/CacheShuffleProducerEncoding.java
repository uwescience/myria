/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.CacheShuffleProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * 
 */
public class CacheShuffleProducerEncoding extends ShuffleProducerEncoding {
  @Override
  public GenericShuffleProducer construct(final ConstructArgs args) {
    Set<Integer> workerIds = getRealWorkerIds();
    argPf.setNumPartitions(workerIds.size());
    /* calling instead the cacheshuffleproducer */
    CacheShuffleProducer producer =
        new CacheShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
            .integerSetToIntArray(workerIds), argPf);
    if (argBufferStateType != null) {
      if (argBufferStateType instanceof KeepMinValueStateEncoding) {
        producer.setBackupBufferAsMin(((KeepMinValueStateEncoding) argBufferStateType).keyColIndices,
            ((KeepMinValueStateEncoding) argBufferStateType).valueColIndex);
      } else if (argBufferStateType instanceof KeepAndSortOnMinValueStateEncoding) {
        producer.setBackupBufferAsPrioritizedMin(
            ((KeepAndSortOnMinValueStateEncoding) argBufferStateType).keyColIndices,
            ((KeepAndSortOnMinValueStateEncoding) argBufferStateType).valueColIndex);
      } else if (argBufferStateType instanceof DupElimStateEncoding) {
        producer.setBackupBufferAsDupElim();
      } else if (argBufferStateType instanceof SimpleAppenderStateEncoding) {
        producer.setBackupBufferAsAppender();
      }
    }
    return producer;
  }
}
