package edu.washington.escience.myriad.parallel.ipc;

import java.util.EventListener;

/**
 * IPC event listener, for calling back when a specific IOEvent is fired.
 * 
 * @param <ATT> IPCEvent attachment type.
 * */
public interface IPCEventListener<ATT> extends EventListener {

  /**
   * An IPCEvent is fired.
   * 
   * @param event the event instance.
   * */
  void triggered(IPCEvent<ATT> event);
}
