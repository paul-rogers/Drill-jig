package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor.*;

/**
 * Field value accessor backed by a Java object. The caller is responsible
 * for calling only the accessor that corresponds to the object's type.
 */

public class BoxedAccessor implements BooleanAccessor, Int8Accessor,
    Int16Accessor, Int32Accessor, Int64Accessor, Float32Accessor,
    Float64Accessor, DecimalAccessor, StringAccessor, ObjectAccessor {

  protected final ObjectAccessor accessor;

  public BoxedAccessor( ObjectAccessor accessor ) {
    this.accessor = accessor;
  }
  
  @Override
  public boolean isNull() {
    return accessor.isNull();
  }

  @Override
  public boolean getBoolean() {
    return (Boolean) getObject( );
  }

  @Override
  public byte getByte() {
    return (Byte) getObject( );
  }

  @Override
  public short getShort() {
    return (Short) getObject( );
  }

  @Override
  public int getInt() {
    return (Integer) getObject( );
  }

  @Override
  public long getLong() {
    return (Long) getObject( );
  }

  @Override
  public float getFloat() {
    return (Float) getObject( );
  }

  @Override
  public double getDouble() {
    return (Double) getObject( );
  }

  @Override
  public BigDecimal getDecimal() {
    return (BigDecimal) getObject( );
  }

  @Override
  public String getString() {
    return (String) getObject( );
  }

  @Override
  public Object getObject() {
    return accessor.getObject();
  }
  
  /**
   * Extends the boxed accessor to convert the boxed object to the corresponding
   * Jig type using the factory provided. Used when the boxed object participates
   * in a Variant field.
   */
 
  public static class VariantBoxedAccessor extends BoxedAccessor implements TypeAccessor
  {
    private final FieldValueFactory factory;

    public VariantBoxedAccessor( ObjectAccessor accessor, FieldValueFactory factory ) {
      super( accessor );
      this.factory = factory;
    }

    @Override
    public DataType getType() {
      return factory.objectToJigType( accessor.getObject( ) );
    }
  }
}
