package org.apache.drill.jig.serde;

import java.math.BigDecimal;

import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.Batch;
import org.junit.Test;

/**
 * Uses the Array data source and the simple buffer reader to test
 * the details of serializing and deserializing each supported Jig
 * type. The test simply writes a set of tuples into a buffer,
 * then immediately deserializes the buffer, without any network
 * traffic.
 */

public class SerdeSimpleTypesTest {

  @Test
  public void testBoolean() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { true },
          { false }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableBoolean() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { true },
          { null },
          { false }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testByte() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (byte) 0 },
          { Byte.MAX_VALUE },
          { Byte.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }


  @Test
  public void testNullableByte() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (byte) 0 },
          { null },
          { Byte.MAX_VALUE },
          { Byte.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testInt16() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (short) 0 },
          { Short.MAX_VALUE },
          { Short.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableInt16() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (short) 0 },
          { null },
          { Short.MAX_VALUE },
          { Short.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testInt32() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { 0 },
          { Integer.MAX_VALUE },
          { Integer.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }


  @Test
  public void testNullableInt32() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { 0 },
          { null },
          { Integer.MAX_VALUE },
          { Integer.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testInt64() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { 0L },
          { Long.MAX_VALUE },
          { Long.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableInt64() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { 0L },
          { null },
          { Long.MAX_VALUE },
          { Long.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testFloat32() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { 0f },
          { Float.MAX_VALUE },
          { Float.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableFloat32() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { 0f },
          { null },
          { Float.MAX_VALUE },
          { Float.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testFloat64() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { 0d },
          { Double.MAX_VALUE },
          { Double.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableFloat64() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { 0d },
          { null },
          { Double.MAX_VALUE },
          { Double.MIN_VALUE }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }


  @Test
  public void testDecimal() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { BigDecimal.ZERO },
          { BigDecimal.ONE },
          { new BigDecimal( Long.MAX_VALUE ).multiply( new BigDecimal( 100 ) ) }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableDecimal() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { BigDecimal.ZERO },
          { null },
          { BigDecimal.ONE },
          { new BigDecimal( Long.MAX_VALUE ).multiply( new BigDecimal( 100 ) ) }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testString() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { "" },
          { " mumble " },
          { "foo\n  bar" }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testNullableString() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { "" },
          { null },
          { " mumble " },
          { "foo\n  bar" }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

  @Test
  public void testVariant() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { "mumble" },
          { null },
          { 10 },
          { BigDecimal.ONE },
          { 10d }
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

}
