package edu.washington.escience.myriad.parallel;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import edu.washington.escience.myriad.table._TupleBatch;

public abstract class Consumer extends Exchange {

  private static final long serialVersionUID = 1L;
  /**
   * The buffer for receiving ExchangeMessages. This buffer should be assigned by the Worker. Basically, buffer =
   * Worker.inBuffer.get(this.getOperatorID())
   * */
  private transient volatile LinkedBlockingQueue<_TupleBatch> inputBuffer;
  protected volatile _TupleBatch outputBuffer;
  
  public Consumer(ExchangePairID oID) {
    super(oID);
  }

  /**
   * Read a single ExchangeMessage from the queue that buffers incoming ExchangeMessages.
   * 
   * @param timeout Wait for at most timeout milliseconds. If the timeout is negative, wait until an element arrives.
   * */
  public _TupleBatch take(int timeout) throws InterruptedException {

    if (timeout >= 0)
      return inputBuffer.poll(timeout, TimeUnit.MILLISECONDS);
    else
      return inputBuffer.take();
  }

  public void setInputBuffer(LinkedBlockingQueue<_TupleBatch> buffer) {
    this.inputBuffer = buffer;
  }
  
  public void setOutputBuffer(_TupleBatch outputBuffer)
  {
    this.outputBuffer = outputBuffer;
  }
  
  public _TupleBatch getOutputBuffer()
  {
    return this.outputBuffer;
  }

}
