package org.apache.drill.jig.types;

import org.apache.drill.jig.types.FieldAccessor.*;

public class BoxedAccessor implements BooleanAccessor, Int8Accessor, Int16Accessor, Int32Accessor, Int64Accessor, Float32Accessor, Float64Accessor, StringAccessor, ObjectAccessor {

  private ObjectAccessor accessor;

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
  public String getString() {
    return (String) getObject( );
  }

  @Override
  public Object getObject() {
    return accessor.getObject();
  }
}
