package edu.washington.escience.myria.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.HashDistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.RoundRobinDistributeFunction;

public class SerializationTests {

  public static ObjectMapper mapper;

  @BeforeClass
  public static void setUp() {
    mapper = MyriaJsonMapperProvider.getMapper();
  }

  @Test
  public void testDistributeFunction() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(DistributeFunction.class);
    String serialized;
    DistributeFunction deserialized;

    /* Multi-field hash */
    int multiFieldIndex[] = new int[] {3, 4, 2};
    DistributeFunction df = new HashDistributeFunction(multiFieldIndex);
    serialized = mapper.writeValueAsString(df);
    deserialized = reader.readValue(serialized);
    assertEquals(df.getClass(), deserialized.getClass());
    HashDistributeFunction pfMFH = (HashDistributeFunction) deserialized;
    assertArrayEquals(multiFieldIndex, pfMFH.getIndexes());

    /* RoundRobin */
    df = new RoundRobinDistributeFunction();
    serialized = mapper.writeValueAsString(df);
    deserialized = reader.readValue(serialized);
    assertEquals(df.getClass(), deserialized.getClass());
  }

  @Test
  public void testDistributeFunctionWithNullNumPartitions() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(DistributeFunction.class);
    HashDistributeFunction df = new HashDistributeFunction(new int[] {3});
    String serialized = mapper.writeValueAsString(df);
    DistributeFunction deserialized = reader.readValue(serialized);
    assertEquals(df.getClass(), deserialized.getClass());
    assertEquals(3, ((HashDistributeFunction) deserialized).getIndexes()[0]);
  }
}
