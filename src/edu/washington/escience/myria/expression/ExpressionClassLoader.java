package edu.washington.escience.myria.expression;

import com.google.common.collect.ImmutableSet;

/**
 * The Java {@link ClassLoader} to be used when compiling expressions. The goal of this object is to whitelist the
 * classes that can be compiled.
 */
public final class ExpressionClassLoader extends ClassLoader {
  /** A whitelist of Java package names. */
  private final ImmutableSet<String> whitelist = ImmutableSet.of("int", "boolean");

  /** Disallow public construction. */
  private ExpressionClassLoader() {
  }

  /**
   * Load the requested class. Only succeeds if the class is in the whitelist.
   * 
   * {@inheritDoc}
   */
  @Override
  public Class<?> findClass(final String name) throws ClassNotFoundException {
    if (whitelist.contains(name)) {
      return super.findClass(name);
    }
    System.err.println("throwing the error for " + name);
    throw new ClassNotFoundException(name + " is not in the whitelist.");
  }

  /**
   * The singleton ExpressionClassLoader.
   */
  private static final ExpressionClassLoader INSTANCE = new ExpressionClassLoader();

  /**
   * @return an ExpressionClassLoader.
   */
  public static ExpressionClassLoader getInstance() {
    return INSTANCE;
  }
}
