package org.apache.drill.jig.serde.serializer;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.serde.BaseTupleSetSerde;

/**
 * Serialize a tuple represented using the Tuple API. Each type has its
 * own serializer that converts a field, given as a generic
 * {@link FieldValue} into the specified form using the
 * {@link TupleWriter}.
 */

public class TupleSetSerializer extends BaseTupleSetSerde
{
  public interface FieldSerializer
  {
    void serialize( TupleWriter writer, FieldValue field );
  }
  
  public static class SerializeString implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeString( field.getString() );
    }   
  }
  
  public static class SerializeBoolean implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeBoolean( field.getBoolean() );
    }   
  }
  
  public static class SerializeByte implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeByte( field.getByte() );
    }   
  }
  
  public static class SerializeShort implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeShort( field.getShort() );
    }   
  }
  
  public static class SerializeInt implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeIntEncoded( field.getInt() );
    }   
  }
  
  public static class SerializeLong implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeLongEncoded( field.getLong() );
    }   
  }
  
  public static class SerializeFloat implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeFloat( field.getFloat() );
    }   
  }
  
  public static class SerializeDouble implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeDouble( field.getDouble() );
    }   
  }
  
  public static class SerializeDecimal implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeDecimal( field.getDecimal() );
    }   
  }
  
  /**
   * Null serializer for arrays and maps.
   */
  
  public static class SerializeNull implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
    }   
  }
  
  /**
   * Variant is special: the serialized form includes the type, then the
   * value serialized in the format for that type.
   */
  
  public static class SerializeVariant implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      DataType type = field.type();
      writer.writeByte( (byte) type.typeCode( ) );
      getScalarSerializer( type ).serialize( writer, field );
    }   
  }
  
  public static class SerializeScalarArray implements FieldSerializer
  {
    private FieldSerializer serializer;
    
    public SerializeScalarArray( FieldSerializer serializer ) {
      this.serializer = serializer;
    }  
    
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      int startPosn = writer.startBlock( );
      ArrayValue array = field.getArray();
      int n = array.size( );
      writer.writeIntEncoded( n );
      if ( array.memberIsNullable( ) ) {
        writeNullFlags( writer, array );
      }
      for ( int i = 0;  i < n;  i++ ) {
        FieldValue member = array.get(i);
        if ( ! member.isNull() )
          serializer.serialize( writer, member );
      }
      writer.endBlock( startPosn );
    }
    
  }
  
  /**
   * Serializes a variant array. A variant array, even if nullable, does not
   * use null flags. Instead, each member is preceded by a type. The NULL
   * type is written for null values (but no data follows the type.)
   */
  
  public static class SerializeVariantArray implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      int startPosn = writer.startBlock( );
      ArrayValue array = field.getArray();
      int n = array.size( );
      writer.writeIntEncoded( n );
      for ( int i = 0;  i < n;  i++ ) {
        writeVariant( writer, array.get(i) );
      }
      writer.endBlock( startPosn );
    }
  }
  
  public static class SerializeMap implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      int startPosn = writer.startBlock( );
      MapValue map = field.getMap();
      writer.writeIntEncoded( map.size() );
      for ( String key : map.keys() ) {
        writer.writeString( key );
        writeVariant( writer, map.get( key ) );
      }
      writer.endBlock( startPosn );
    }
  }
  
  private final static FieldSerializer serializerByType[] = initSerializers( );
  
  private TupleWriter writer = new TupleWriterV1( );
  private final FieldSerializer serializer[];
  
  /**
   * Set up various tables used to speed serialization.
   */
  
  public TupleSetSerializer( TupleSchema schema ) {
    super( schema );
    serializer = new FieldSerializer[ fieldCount ];
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldSchema field = schema.field( i );
      serializer[i] = serializerForSchema( field );
    }
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
    serializers[ DataType.VARIANT.ordinal() ] = new SerializeVariant( );
    serializers[ DataType.NUMBER.ordinal() ] = new SerializeVariant( );
    serializers[ DataType.NULL.ordinal() ] = new SerializeNull( );
    serializers[ DataType.UNDEFINED.ordinal() ] = new SerializeNull( );
//    serializers[ DataType.LIST.ordinal() ] = new SerializeArray( serializers );
    serializers[ DataType.MAP.ordinal() ] = new SerializeMap( );
    return serializers;
  }
  
  public static FieldSerializer getScalarSerializer( DataType type ) {
    return serializerByType[ type.typeCode() ];
  }

  private FieldSerializer serializerForSchema(FieldSchema field) {
    if ( field.type() != DataType.LIST ) {
      return getScalarSerializer( field.type() );
    }
    FieldSchema member = field.member();
    if ( member.type( ).isVariant() )
      return new SerializeVariantArray( );
    else
      return new SerializeScalarArray( serializerForSchema( member ) );
  }

  /**
   * Write a tuple to the give buffer as a length, a header of null & repeat
   * bits, and the encoded values (but only for non-null, non-repeated fields.)
   * 
   * @param buf
   * @param tuple
   */
  
  public boolean serializeTuple( ByteBuffer buf, TupleValue tuple ) {
    buf.mark();
    try
    {
      scanFields( tuple );
      writer.bind( buf );
      int startPosn = writer.startBlock( );
      writer.writeHeader( isNull, isRepeated );
      writeFields( tuple );
      writer.endBlock( startPosn );
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
  
  private void scanFields(TupleValue tuple) {
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldValue field = tuple.field( i );
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
  
  private void writeFields(TupleValue tuple) {
    for ( int i = 0;  i < fieldCount;  i++ ) {
      if ( isNull[i] || isRepeated[i] )
        continue;
      FieldValue field = tuple.field( i );
      serializer[i].serialize( writer, field );
    }
  }
  
  /**
   * Writes null flags for the array members. Flags are of the
   * form:<br>
   * <pre>[ n0 n1 ... n6 n7 ][ n8 0 ... 0 ]</pre></br>
   * That is, flags are packed starting with the high-order
   * bit downwards, with unused low-order bits set to 0.
   * 
   * @param writer
   * @param array
   */
  protected static void writeNullFlags( TupleWriter writer, ArrayValue array ) {
    int n = array.size( );
    int byteCount = (n+7)/8;
    int posn = 0;
    for ( int i = 0;  i < byteCount;  i++ ) {
      int mask = 0x80;
      int flags = 0;
      for ( int j = 0;  j < 8  &&  posn < n;  j++ ) {
        if ( array.get(posn++).isNull())
          flags |= mask;
        mask >>= 1;
      }
      writer.writeByte( (byte) flags );
    }
  }
  
  protected static void writeVariant( TupleWriter writer, FieldValue value ) {
    int typeCode = value.type( ).ordinal();
    writer.writeByte( (byte) typeCode );
    FieldSerializer fieldSerializer = serializerByType[ typeCode ];
    fieldSerializer.serialize( writer, value );
  }
}
