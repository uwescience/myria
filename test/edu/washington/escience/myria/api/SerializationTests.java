package edu.washington.escience.myria.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import edu.washington.escience.myria.parallel.MultiFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.PartitionFunction;
import edu.washington.escience.myria.parallel.RoundRobinPartitionFunction;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.WholeTupleHashPartitionFunction;

public class SerializationTests {

  public static ObjectMapper mapper;

  @BeforeClass
  public static void setUp() {
    mapper = MyriaJsonMapperProvider.getMapper();
  }

  @Test
  public void testPartitionFunction() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(PartitionFunction.class);
    PartitionFunction pf;
    String serialized;
    PartitionFunction deserialized;

    /* Single field hash */
    pf = new SingleFieldHashPartitionFunction(5, 3);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numPartition());
    SingleFieldHashPartitionFunction pfSFH = (SingleFieldHashPartitionFunction) deserialized;
    assertEquals(3, pfSFH.getIndex());

    /* Multi-field hash */
    int multiFieldIndex[] = new int[] { 3, 4, 2 };
    pf = new MultiFieldHashPartitionFunction(5, multiFieldIndex);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numPartition());
    MultiFieldHashPartitionFunction pfMFH = (MultiFieldHashPartitionFunction) deserialized;
    assertArrayEquals(multiFieldIndex, pfMFH.getFieldIndexes());

    /* Whole tuple hash */
    pf = new WholeTupleHashPartitionFunction(5);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numPartition());

    /* RoundRobin */
    pf = new RoundRobinPartitionFunction(5);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numPartition());
  }

}
