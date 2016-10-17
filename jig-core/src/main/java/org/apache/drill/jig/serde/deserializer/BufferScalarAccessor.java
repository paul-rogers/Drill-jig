package org.apache.drill.jig.serde.deserializer;

import java.math.BigDecimal;

import org.apache.drill.jig.accessor.FieldAccessor.*;
import org.apache.drill.jig.api.DataType;

public abstract class BufferScalarAccessor extends BufferAccessor
    implements BooleanAccessor, Int8Accessor, Int16Accessor, Int32Accessor,
    Int64Accessor, Float32Accessor, Float64Accessor, DecimalAccessor, StringAccessor {

  protected abstract void seek( );
  
  @Override
  public boolean getBoolean() {
    seek( );
    return reader.readBoolean();
  }   
  
  @Override
  public byte getByte() {
    seek( );
    return reader.readByte();
  }
  
  @Override
  public short getShort() {
    seek( );
    return reader.readShort();
  }

  @Override
  public int getInt() {
    seek( );
    return reader.readIntEncoded();
  }

  @Override
  public long getLong() {
    seek( );
    return reader.readLongEncoded();
  }
  
  @Override
  public float getFloat() {
    seek( );
    return reader.readFloat();
  }
  
  @Override
  public double getDouble() {
    seek( );
    return reader.readDouble();
  }
  
  @Override
  public BigDecimal getDecimal() {
    seek( );
    return reader.readDecimal();
  }
  
  @Override
  public String getString() {
    seek( );
    return reader.readString();
  }
  
  public static class BufferScalarFieldAccessor extends BufferScalarAccessor
  {
    protected int index;
    protected TupleSetDeserializer deserializer;
    
    public void bind( TupleSetDeserializer deserializer, int index ) {
      this.deserializer = deserializer;
      this.index = index;
      super.bind( deserializer.reader( ) );
    }
    
    @Override
    public boolean isNull() {
      return deserializer.isNull( index );
    }
    
    protected void seek( ) {
      deserializer.seek( index );
    }
  }
  
  public static class BufferVariantFieldAccessor extends BufferScalarFieldAccessor implements TypeAccessor
  {
    @Override
    public DataType getType() {
      deserializer.seek( index );
      return DataType.typeForCode( reader.readByte() );
    }   
    
    protected void seek( ) {
      deserializer.seekVariant( index );
    }
  }
  
  public static class BufferMemberAccessor extends BufferScalarFieldAccessor {
    
    protected void seek( ) { }
  }
}
