package org.apache.drill.jig.serde;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldAccessor;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleAccessor;
import org.apache.drill.jig.api.TupleSchema;

/**
 * Serialize a tuple represented using the Tuple API. Each type has its
 * own serializer that converts a field, given as a generic
 * {@link FieldAccessor} into the specified form using the
 * {@link TupleWriter}.
 */

public class TupleSetSerializer extends BaseTupleSetSerde
{
  public interface FieldSerializer
  {
    void serialize( TupleWriter writer, FieldAccessor field );
  }
  
  public static class SerializeString implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeString( field.asScalar().getString() );
    }   
  }
  
  public static class SerializeBoolean implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeBoolean( field.asScalar().getBoolean() );
    }   
  }
  
  public static class SerializeByte implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeByte( field.asScalar().getByte() );
    }   
  }
  
  public static class SerializeShort implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeShort( field.asScalar().getShort() );
    }   
  }
  
  public static class SerializeInt implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeIntEncoded( field.asScalar().getInt() );
    }   
  }
  
  public static class SerializeLong implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeLongEncoded( field.asScalar().getLong() );
    }   
  }
  
  public static class SerializeFloat implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeFloat( field.asScalar().getFloat() );
    }   
  }
  
  public static class SerializeDouble implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeDouble( field.asScalar().getDouble() );
    }   
  }
  
  public static class SerializeDecimal implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeDecimal( field.asScalar().getDecimal() );
    }   
  }
  
  /**
   * An Any type is special: the serialized form includes the type, then the
   * value serialized in the format for that type.
   */
  
  public static class SerializeAny implements FieldSerializer
  {
    private FieldSerializer[] serializers;
    
    public SerializeAny( FieldSerializer serializers[] ) {
      this.serializers = serializers;
    }
    
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      DataType type = field.asAny().getDataType();
      writer.writeByte( (byte) type.typeCode( ) );
      serializers[ type.ordinal() ].serialize( writer, field );
    }   
  }
  
  private static FieldSerializer serializerByType[] = initSerializers( );
  
  private TupleWriter writer = new TupleWriterV1( );
  private FieldSerializer serializer[];
  
  public TupleSetSerializer( TupleSchema schema ) {
    this.schema = schema;
    prepare( );
  }
  
  /**
   * Build a table of serializer by (in-memory) type. This table
   * isolates the wire format (which must be stable) from any changes
   * to the {@link DataType} enum, which may evolve with the
   * code.
   * 
   * @return
   */
  
  private static FieldSerializer[] initSerializers() {
    int count = DataType.values().length;
    FieldSerializer serializers[] = new FieldSerializer[ count ];
    serializers[ DataType.BOOLEAN.ordinal() ] = new SerializeBoolean( );
    serializers[ DataType.INT8.ordinal() ] = new SerializeByte( );
    serializers[ DataType.INT16.ordinal() ] = new SerializeShort( );
    serializers[ DataType.INT32.ordinal() ] = new SerializeInt( );
    serializers[ DataType.INT64.ordinal() ] = new SerializeLong( );
    serializers[ DataType.FLOAT32.ordinal() ] = new SerializeFloat( );
    serializers[ DataType.FLOAT64.ordinal() ] = new SerializeDouble( );
    serializers[ DataType.DECIMAL.ordinal() ] = new SerializeDecimal( );
    serializers[ DataType.STRING.ordinal() ] = new SerializeString( );
    serializers[ DataType.ANY.ordinal() ] = new SerializeAny( serializers );
    return serializers;
  }

  /**
   * Set up various tables used to speed serialization.
   */
  
  private void prepare( ) {
    super.prepare( schema.getCount( ) );
    serializer = new FieldSerializer[ fieldCount ];
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldSchema field = schema.getField( i );
      serializer[i] = serializerByType[ field.getType().ordinal() ];
    }
  }
  
  /**
   * Write the schema to the buffer as a length, a field count and
   * a (name, type, cardinality) triple for each field.
   * 
   * @param buf
   */
  
  public void serializeSchema( ByteBuffer buf ) {
    writer.startBlock( buf );
    writer.writeIntEncoded( fieldCount );
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldSchema field = schema.getField( i );
      writer.writeString( field.getName() );
      writer.writeByte( (byte) field.getType().typeCode( ) );
      writer.writeByte( (byte) field.getCardinality().cardinalityCode( ) );
    }
    writer.endBlock( );
  }
  
  /**
   * Write a tuple to the give buffer as a length, a header of null & repeat
   * bits, and the encoded values (but only for non-null, non-repeated fields.)
   * 
   * @param buf
   * @param tuple
   */
  
  public boolean serializeTuple( ByteBuffer buf, TupleAccessor tuple ) {
    buf.mark();
    try
    {
      scanFields( tuple );
      writer.startBlock( buf );
      writer.writeHeader( isNull, isRepeated );
      writeFields( tuple );
      writer.endBlock();
      return true;
    }
    catch ( IndexOutOfBoundsException e ) {
      buf.reset();
      return false;
    }
  }

  /**
   * Determine which fields are null (or, later, repeated.)
   * <p>
   * Note that this work could be done while serializing the field.
   * If done that way, we'd back-patch the header rather than writing
   * the header then values.
   * 
   * @param tuple
   */
  
  private void scanFields(TupleAccessor tuple) {
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldAccessor field = tuple.getField( i );
      isNull[i] = field.isNull();
    }
  }

  /**
   * Write the set of fields using the null/repeat flags to determine
   * which to skip, and the encoding method specified in the field
   * serializer.
   * 
   * @param tuple
   */
  
  private void writeFields(TupleAccessor tuple) {
    for ( int i = 0;  i < fieldCount;  i++ ) {
      if ( isNull[i] || isRepeated[i] )
        continue;
      FieldAccessor field = tuple.getField( i );
      serializer[i].serialize( writer, field );
    }
  }
}
