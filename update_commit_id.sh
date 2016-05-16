#!/bin/bash

cur_commit="$(git rev-parse HEAD)"
cur_branch="$(git rev-parse --abbrev-ref HEAD)"

echo "package edu.washington.escience.myria;

/**
 * Version info about the source code at build time.
 */
public final class MyriaCommit {
  /** Utility class cannot be constructed. */
  private MyriaCommit() {}

  /** The git commit id at build time. */
  public static final String COMMIT = \"$cur_commit\";
  /** The git branch at build time. */
  public static final String BRANCH = \"$cur_branch\";
}" > src/edu/washington/escience/myria/MyriaCommit.java
