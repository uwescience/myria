package edu.washington.escience.myriad.parallel.ipc;

import java.util.EventListener;

/**
 * IPC event listener, for calling back when a specific IOEvent is fired.
 * 
 * */
public interface IPCEventListener extends EventListener {

  /**
   * An IPCEvent is fired.
   * 
   * @param event the event instance.
   * */
  void triggered(IPCEvent event);
}
