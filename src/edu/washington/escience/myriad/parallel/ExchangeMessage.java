package edu.washington.escience.myriad.parallel;

import java.io.Serializable;

import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

/**
 * All the messages that will change between Exchange Operators should be a sub class of ExchangeMessage.
 * 
 */
public interface ExchangeMessage extends Serializable {

  // private static final long serialVersionUID = 1L;

  // private ExchangePairID operatorID;
  // private String fromWorkerID;

  // public ExchangeMessage(ExchangePairID oID, String workerID) {
  // this.operatorID = oID;
  // this.fromWorkerID = workerID;
  // }

  /**
   * Get the ParallelOperatorID, to which this message is targeted
   */
  ExchangePairID getOperatorID();

  /**
   * Get the worker id from which the message was sent
   */
  int getWorkerID();

  // public void setOperatorID(ExchangePairID poid) {
  // this.operatorID = poid;
  // }
  //
  // public void setWorkerID(String workerID) {
  // this.fromWorkerID = workerID;
  // }
}
