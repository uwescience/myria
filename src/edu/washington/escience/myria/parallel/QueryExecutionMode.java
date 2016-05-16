package edu.washington.escience.myria.parallel;

/**
 * query execution mode, blocking or non-blocking. Always use the NON_BLOCKING mode. The BLOCKING mode may not work and
 * may get abandoned.
 * */
public enum QueryExecutionMode {
  /**
   * blocking execution, call next() and fetchNext().
   * */
  BLOCKING,

  /**
   * non-blocking execution, call nextReady() and fetchNextReady().
   * */
  NON_BLOCKING;
}
