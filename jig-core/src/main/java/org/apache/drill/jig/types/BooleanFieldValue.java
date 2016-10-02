package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor.BooleanAccessor;

public class BooleanFieldValue extends AbstractScalarFieldValue {

  private BooleanAccessor accessor;
  
  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (BooleanAccessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.BOOLEAN;
  }

  @Override
  public boolean isNull() {
    return accessor.isNull();
  }
  
  @Override
  public boolean getBoolean() {
    return accessor.getBoolean( );
  }

  @Override
  public byte getByte() {
    return (byte) getInt( );
  }

  @Override
  public short getShort() {
    return (short) getInt( );
  }

  @Override
  public int getInt() {
    return getBoolean( ) ? 1 : 0;
  }

  @Override
  public long getLong() {
    return getInt( );
  }

  @Override
  public float getFloat() {
    return getInt( );
  }

  @Override
  public double getDouble() {
    return getInt( );
  }

  @Override
  public BigDecimal getDecimal() {
    return getBoolean( ) ? BigDecimal.ONE : BigDecimal.ZERO;
  }

  @Override
  public String getString() {
    return Boolean.toString( getBoolean( ) );
  }

  @Override
  public Object getValue() {
    return getBoolean( );
  }
}
