/**
 *
 * Annotation for describing a field is required in the JSON Query/Dataset encoding.
 *
 * @author slxu
 */
package edu.washington.escience.myria.api.encoding;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Required {}
