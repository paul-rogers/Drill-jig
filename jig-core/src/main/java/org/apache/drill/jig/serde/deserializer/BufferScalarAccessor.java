package org.apache.drill.jig.serde.deserializer;

import java.math.BigDecimal;

import org.apache.drill.jig.accessor.FieldAccessor.*;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.util.JigUtilities;

/**
 * Accessor to read simple "scalar" values from the serialized buffer.
 * Different forms exist for top-level and nested fields, and for
 * top-level variant fields.
 */

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

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "]" );
  }

  /**
   * Reads a scalar field value. Field values are randomly accessed on
   * demand by the client, so each read is prefaced with a seek in the
   * buffer to the desired value position.
   */

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

    @Override
    protected void seek( ) {
      deserializer.seek( index );
    }

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      buf.append( " index = " );
      buf.append( index );
      buf.append( "]" );
    }
  }

  /**
   * Accessor to read a variant. A variant is written as a combination of
   * one-byte type code, followed by value serialized according to that
   * type. Seeking to the value must skip the type code.
   */

  public static class BufferVariantFieldAccessor extends BufferScalarFieldAccessor implements TypeAccessor
  {
    @Override
    public DataType getType() {
      deserializer.seek( index );
      return DataType.typeForCode( reader.readByte() );
    }

    @Override
    protected void seek( ) {
      deserializer.seekVariant( index );
    }
  }

  /**
   * Accessor that reads array element or map values which are stored in
   * the buffer sequentially. No seeking is done to read the sequential
   * values.
   */

  public static class BufferMemberAccessor extends BufferScalarFieldAccessor {

    @Override
    protected void seek( ) { }
  }
}
