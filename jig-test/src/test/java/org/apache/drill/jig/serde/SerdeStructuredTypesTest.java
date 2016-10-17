package org.apache.drill.jig.serde;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.Batch;
import org.junit.Test;

public class SerdeStructuredTypesTest {

  @Test
  public void testSimpleMap() throws JigException {
    Map<String,Integer> map1 = new HashMap<>( );
    map1.put( "one", 1 );
    map1.put( "two", 2 );
    map1.put( "three", 3 );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { map1 }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testFancyMap() throws JigException {
    Map<String,Object> map1 = new HashMap<>( );
    map1.put( "one", 1 );
    map1.put( "two", 2L );
    map1.put( "three", 3d );
    map1.put( "four", BigDecimal.ONE );
    map1.put( "five", null );
    map1.put( "six", "mumble" );
    Map<String,Object> map2 = new HashMap<>( );
    map2.put( "a", "a-value" );
    map2.put( "b", 10 );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { map1 },
          { null },
          { map2 }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testPrimitiveInt8Array() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new byte[ ] { (byte) 0, Byte.MIN_VALUE, Byte.MAX_VALUE } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testPrimitiveInt8NullableArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new byte[ ] { (byte) 0, Byte.MIN_VALUE, Byte.MAX_VALUE } },
          { null },
          { new byte[] { (byte) 1, (byte) 2 } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testPrimitiveInt16Array() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new short[ ] { (short) 0, Short.MIN_VALUE, Short.MAX_VALUE } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testPrimitiveInt32Array() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new int[ ] { 0, Integer.MIN_VALUE, Integer.MAX_VALUE } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testPrimitiveInt64Array() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new long[ ] { 0, Long.MIN_VALUE, Long.MAX_VALUE } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testPrimitiveFloat32Array() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new float[ ] { 0, Float.MIN_VALUE, Float.MAX_VALUE } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testPrimitiveFloat64Array() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new double[ ] { 0, Double.MIN_VALUE, Double.MAX_VALUE } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testStringArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new String[ ] { "a", "b", "mumble" } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableStringArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new String[ ] { "a", "b", null, "mumble" } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableStringNullableArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new String[ ] { "a", "b", null, "mumble" } },
          { null },
          { new String[] { "foo", null, "bar" } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testDecimalArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new BigDecimal[ ] { BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.TEN } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testVariantArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[ ] { "foo", 100, BigDecimal.ZERO } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableVariantArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[ ] { "foo", 100, null, BigDecimal.ZERO } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testVariantNullableArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[ ] { "foo", 100, BigDecimal.ZERO } },
          { null },
          { new Object[ ] { 200, "mumble", BigDecimal.TEN } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableVariantNullableArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[ ] { "foo", 100, null, BigDecimal.ZERO } },
          { null },
          { new Object[ ] { 200, "mumble", null, BigDecimal.TEN } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testArrayOfPrimitiveArray() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[ ] { 
              new int[] {1, 2, 3},
              new int[] {10, 20, 30, 40},
              new int[] {100, 200}
              } },
          { new Object[ ] { 
              new int[] {11, 21, 31, 41},
              } }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }
}
