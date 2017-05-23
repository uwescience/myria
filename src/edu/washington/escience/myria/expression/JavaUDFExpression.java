package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Iterator;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.NoSuchMethodException;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 *
 * Java UDF expressions.
 *
 */
public class JavaUDFExpression extends NAryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  @JsonProperty private String name;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private JavaUDFExpression() {}
  /**
   * Java function expression.
   *
   * @param children operand.
   * @param name - name of the java function in the catalog
   */
  public JavaUDFExpression(final List<ExpressionOperator> children, final String name) {
    super(children);
    this.name = name;
  }

  // Returns true if c1 can be implicitly cast as c2
  private static boolean castable(Class<?> c1, Class<?> c2) {
    if (c2.equals(c1)) {
      return true;
    } else if (c2.equals(boolean.class) || c2.equals(Boolean.class)) {
      if (c1.equals(Boolean.class)) {
        return true;
      }
    } else if (c2.equals(int.class) || c2.equals(Integer.class)) {
      if (c1.equals(Integer.class)) {
        return true;
      }
    } else if (c2.equals(long.class) || c2.equals(Long.class)) {
      if (c1.equals(Integer.class) || c1.equals(Long.class)) {
        return true;
      }
    } else if (c2.equals(float.class) || c2.equals(Float.class)) {
      if (c1.equals(Float.class)) {
        return true;
      }
    } else if (c2.equals(double.class) || c2.equals(Double.class)) {
      if (c1.equals(Float.class) || c1.equals(Double.class)) {
        return true;
      }
    }
    return false;
  }

  private static boolean compareTypes(Class<?>[] parameterTypes, Type[] childrenTypes) {
    for (int i = 0; i < parameterTypes.length; i++) {
      if (!castable(childrenTypes[i].toJavaObjectType(), parameterTypes[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    int index = this.name.lastIndexOf(".");

    Preconditions.checkArgument(
        index != -1 && index != this.name.length(),
        "%s should be in the form className.methodName",
        this.name);

    String className = this.name.substring(0, index);
    String methodName = this.name.substring(index + 1);

    int childrenSize = getChildren() != null ? getChildren().size() : 0;

    Type[] childrenTypes = new Type[childrenSize];
    for (int i = 0; i < childrenSize; i++) {
      childrenTypes[i] = getChild(i).getOutputType(parameters);
    }

    Class<?> c = null;

    try {
      c = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("ClassNotFoundException caused by class " + className);
    }

    // Search through class methods for matching signature
    Method[] methods = c.getDeclaredMethods();
    for (Method m : methods) {
      // Check that the current method name is equal to the one that we are searching for and that this method is static
      if (!m.getName().equals(methodName) || !Modifier.isStatic(m.getModifiers())) {
        continue;
      }

      // Check that the length of the parameters are equivalent
      Class<?>[] parameterTypes = m.getParameterTypes();
      if (parameterTypes.length != childrenTypes.length) {
        continue;
      }

      // Check that the signature of this method is compatible with the expected signature
      if (!compareTypes(parameterTypes, childrenTypes)) {
        continue;
      }

      Type t = Type.fromJavaType(m.getReturnType());

      // Not a compatible myria type
      if (t == null) {
        break;
      }
      return t;
    }

    // No suitable method was found throw an exception
    StringBuilder signature = new StringBuilder(methodName + "(");
    if (childrenSize != 0) {
      signature.append(childrenTypes[0].toJavaType().getSimpleName());
      for (int i = 1; i < childrenSize; i++) {
        signature.append(childrenTypes[i].toJavaType().getSimpleName());
      }
    }
    signature.append(")");

    throw new IllegalArgumentException("No method found with signature " + signature.toString());
  }

  /**
   * Get this.name(args)
   */
  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    StringBuilder javaString = new StringBuilder(name + "(");

    Iterator<ExpressionOperator> it = getChildren().iterator();
    if (it.hasNext()) {
      javaString.append(it.next().getJavaString(parameters));
    }
    while (it.hasNext()) {
      javaString.append(", ");
      javaString.append(it.next().getJavaString(parameters));
    }

    javaString.append(")");
    return javaString.toString();
  }
}
