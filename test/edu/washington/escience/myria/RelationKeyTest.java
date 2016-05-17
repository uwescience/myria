package edu.washington.escience.myria;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

public class RelationKeyTest {

  /* r1a and r1b are the same */
  RelationKey r1a = RelationKey.of("a", "b", "c");
  RelationKey r1b = RelationKey.of("a", "b", "c");
  /* r2, r3, r4 differ from r1a by one place each */
  RelationKey r2 = RelationKey.of("a", "b", "d");
  RelationKey r3 = RelationKey.of("a", "d", "c");
  RelationKey r4 = RelationKey.of("d", "b", "c");

  @Test
  public void testEquals() {
    assertEquals(r1a, r1b);
    assertNotEquals(r1a, r2);
    assertNotEquals(r1a, r3);
    assertNotEquals(r1a, r4);
    assertNotEquals(r2, r3);
    assertNotEquals(r2, r4);
    assertNotEquals(r3, r4);
  }

  @Test
  public void testHashCode() {
    assertEquals(r1a.hashCode(), r1b.hashCode());
    assertNotEquals(r1a.hashCode(), r2.hashCode());
    assertNotEquals(r1a.hashCode(), r3.hashCode());
    assertNotEquals(r1a.hashCode(), r4.hashCode());
    assertNotEquals(r2.hashCode(), r3.hashCode());
    assertNotEquals(r2.hashCode(), r4.hashCode());
    assertNotEquals(r3.hashCode(), r4.hashCode());
  }

  @Test
  public void testToString() {
    assertEquals("a:b:c", r1a.toString());
    assertEquals("\"a:b:c\"", r1a.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
    assertEquals("\"a:b:c\"", r1a.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL));
    assertEquals("\"a b c\"", r1a.toString(MyriaConstants.STORAGE_SYSTEM_MONETDB));
    assertEquals("`a b c`", r1a.toString(MyriaConstants.STORAGE_SYSTEM_MYSQL));
  }

  @Test
  public void testGoodFieldNames() {
    RelationKey.of("a134", "__a4_323_fg", "z1yxcf_");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFieldNameStartNumber() {
    RelationKey.of("1a", "b", "c");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFieldNameHasHyphen() {
    RelationKey.of("a-", "b", "c");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFieldNameHasSpace() {
    RelationKey.of(" ", "b", "c");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFieldNameHasSpace2() {
    RelationKey.of(" a", "b", "c");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFieldNameHasSpace3() {
    RelationKey.of("a ", "b", "c");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFieldNameHasSpace4() {
    RelationKey.of(" a ", "b", "c");
  }

  @Test(expected = NullPointerException.class)
  public void testBadFieldIsNull() {
    RelationKey.of("a", null, "c");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFieldIsEmpty() {
    RelationKey.of("a", "b", "");
  }
}
