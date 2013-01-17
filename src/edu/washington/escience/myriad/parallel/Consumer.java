package edu.washington.escience.myriad.parallel;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

public abstract class Consumer extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The buffer for receiving ExchangeMessages. This buffer should be assigned by the Worker. Basically, buffer =
   * Worker.inBuffer.get(this.getOperatorID())
   */
  private transient volatile LinkedBlockingQueue<ExchangeData> inputBuffer;

  protected ExchangePairID operatorID;

  public Consumer(final ExchangePairID oID) {
    operatorID = oID;
  }

  public ExchangePairID getOperatorID() {
    return operatorID;
  }

  public void setInputBuffer(final LinkedBlockingQueue<ExchangeData> buffer) {
    inputBuffer = buffer;
  }

  /**
   * Read a single ExchangeMessage from the queue that buffers incoming ExchangeMessages.
   * 
   * @param timeout Wait for at most timeout milliseconds. If the timeout is negative, wait until an element arrives.
   */
  public ExchangeData take(final int timeout) throws InterruptedException {

    if (timeout >= 0) {
      return inputBuffer.poll(timeout, TimeUnit.MILLISECONDS);
    } else {
      return inputBuffer.take();
    }
  }
}
