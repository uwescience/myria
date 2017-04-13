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

  @JsonProperty private Type outputType;

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
  public JavaUDFExpression(
      final List<ExpressionOperator> children, final String name, final Type outputType) {
    super(children);
    this.name = name;
    this.outputType = outputType;
  }

  private static boolean compareTypes(Class<?>[] parameterTypes, Type[] childrenTypes) {
    for (int i = 0; i < parameterTypes.length; i++) {
      if (!parameterTypes[i].equals(childrenTypes[i].toJavaType())
          && !parameterTypes[i].equals(childrenTypes[i].toJavaObjectType())) {
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

    int childrenSize = getChildren().size();

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
      if (!m.getName().equals(methodName) || !Modifier.isStatic(m.getModifiers())) {
        continue;
      }

      Class<?>[] parameterTypes = m.getParameterTypes();
      if (parameterTypes.length != childrenTypes.length) {
        continue;
      }

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

    // No suitable method was found
    StringBuilder signature = new StringBuilder(methodName + "(");
    if (childrenSize != 0) {
      signature.append(childrenTypes[0].toJavaType().getSimpleName());
      for (int i = 1; i < childrenSize; i++) {
        signature.append(childrenTypes[i].toJavaType().getSimpleName());
      }
    }

    signature.append(")");
    throw new IllegalArgumentException("No method found with signature" + signature.toString());
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
