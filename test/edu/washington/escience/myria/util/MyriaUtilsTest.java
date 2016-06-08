/**
 *
 */
package edu.washington.escience.myria.util;

import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import org.joda.time.DateTime;
import org.junit.Test;

/**
 *
 */
public class MyriaUtilsTest {

  @Test
  public void testEnsureObjectIsValidType() {
    MyriaUtils.ensureObjectIsValidType(Boolean.FALSE);
    MyriaUtils.ensureObjectIsValidType(Boolean.TRUE);
    MyriaUtils.ensureObjectIsValidType(false);
    MyriaUtils.ensureObjectIsValidType(new Double(0));
    MyriaUtils.ensureObjectIsValidType(new Float(0));
    MyriaUtils.ensureObjectIsValidType(new Long(0));
    MyriaUtils.ensureObjectIsValidType(new Integer(0));
    MyriaUtils.ensureObjectIsValidType("string");
    MyriaUtils.ensureObjectIsValidType(DateTime.now());
  }

  private void assertInvalidObject(final Object o) {
    try {
      MyriaUtils.ensureObjectIsValidType(o);
      fail();
    } catch (IllegalArgumentException e) {
      /* Expected */
    }
  }

  @Test
  public void testEnsureObjectIsInvalidType() {
    /* Various arrays of valid types */
    assertInvalidObject(new Boolean[] {});
    assertInvalidObject(new Double[] {});
    assertInvalidObject(new Float[] {});
    assertInvalidObject(new Integer[] {});
    assertInvalidObject(new Integer[] {3});
    assertInvalidObject(new Long[] {});
    assertInvalidObject(new Long[] {3L});
    assertInvalidObject(new String[] {"hi", "mom"});
    assertInvalidObject(new DateTime[] {});
    /* Random other objects that seem related. */
    assertInvalidObject(BigInteger.ZERO);
    assertInvalidObject(BigInteger.ONE);
    assertInvalidObject(new Date());
    assertInvalidObject(Calendar.getInstance());
  }

  @Test(expected = NullPointerException.class)
  public void testEnsureObjectIsNullType() {
    MyriaUtils.ensureObjectIsValidType(null);
  }
}
