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
   * Null serializer for array and map elements.
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
  
  /**
   * Serializes an array or map. This class defines structures as nestable. Outer
   * structures are written as:<br>
   * <pre>[ size contents ]</pre>
   * Where the size is an unencoded int32 of the entire array
   * contents. The structure contents are defined by the subclasses.
   * <p>
   * When nested, the serializer omits the size argument, just writing
   * the structure contents (which always includes an element count, not to
   * be confused with the byte size.)
   */
  
  public static abstract class StructureSerializer implements FieldSerializer
  {    
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      int startPosn = writer.startBlock( );
      serializeContents( writer, field );
      writer.endBlock( startPosn );
    }
    
    protected abstract void serializeContents(TupleWriter writer, FieldValue field);
  }
  
  /**
   * Writes a scalar array as:<br>
   * <pre>[ size | count | null flags | values ]</pre>
   * Where the size is an unencoded int32, the count is an
   * encoded in32, the null flags are options, and the
   * values are written in the form required for the element
   * type. Null flags are written only if the array elements
   * are nullable, and indicate the nullability of each element.
   * If an element is null, then no value is written for that
   * element.
   */
  
  public static class SerializeScalarArray extends StructureSerializer
  {
    private FieldSerializer serializer;
    
    public SerializeScalarArray( FieldSerializer serializer ) {
      this.serializer = serializer;
    }  
    
    @Override
    public void serializeContents(TupleWriter writer, FieldValue field) {
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
    }    
  }
  
  /**
   * Serializes a variant array. A variant array, even if nullable, does not
   * use null flags. Instead, each member is preceded by a type. The NULL
   * type is written for null values (but no data follows the type.)
   */
  
  public static class SerializeVariantArray extends StructureSerializer
  {
    @Override
    public void serializeContents(TupleWriter writer, FieldValue field) {
      ArrayValue array = field.getArray();
      int n = array.size( );
      writer.writeIntEncoded( n );
      for ( int i = 0;  i < n;  i++ ) {
        writeVariant( writer, array.get(i) );
      }
    }
  }
  
  /**
   * Serializes an array of maps as:<br>
   * <pre>[ size count [ map0 ] [ map1 ] ... ]</pre>
   * Where the size is an unencoded int32, the count is the element
   * count, and each map is serialized in the map format (without
   * the size header.) If the maps can be null, then writes a
   * null bit mask first and omits null elements.
   */
  
  public static class SerializeMapArray extends StructureSerializer
  {
    @Override
    public void serializeContents(TupleWriter writer, FieldValue field) {
      ArrayValue array = field.getArray();
      int n = array.size( );
      writer.writeIntEncoded( n );
      if ( array.memberIsNullable( ) ) {
        writeNullFlags( writer, array );
      }
      SerializeMap elementSerializer = (SerializeMap) getScalarSerializer( DataType.MAP );
      for ( int i = 0;  i < n;  i++ ) {
        FieldValue element = array.get( i );
        if ( ! element.isNull( ) ) {
          elementSerializer.serializeContents( writer, element );
        }
      }
    }
  }
  
  /**
   * Serializes an array of arrays using the member element serializer
   * provided. Only the top-level array carries the field size, the
   * nested arrays are written without sizes for each member, just the
   * element count. That is, the format is:<br>
   * <pre>[ size count
   *       [ count0 element0.0 element0.1 ... ]
   *       [ count1 element1.0 element1.1 ... ]
   *       ... ]</pre>
   *  Where size is an unencoded int32, count is an encoded int32,
   *  and the serialized contents of the arrays are as defined by
   *  the selected array serializer.
   *  <p>
   *  This class is, itself, a nestable array, though Drill allows
   *  only 2-D arrays, not 3-D or higher.
   */
  
  public static class SerializeArrayOfStructure extends StructureSerializer
  {
    private StructureSerializer elementSerializer;

    public SerializeArrayOfStructure( StructureSerializer elementSerializer ) {
      this.elementSerializer = elementSerializer;
    }
    
    @Override
    public void serializeContents(TupleWriter writer, FieldValue field) {
      ArrayValue array = field.getArray();
      int n = array.size( );
      writer.writeIntEncoded( n );
      if ( array.memberIsNullable( ) ) {
        writeNullFlags( writer, array );
      }
      for ( int i = 0;  i < n;  i++ ) {
        FieldValue element = array.get( i );
        if ( ! element.isNull( ) ) {
          elementSerializer.serializeContents( writer, element );
        }
      }
    }
  }
  
  /**
   * Writes a map as:<br>
   * <pre>[ size count | key1 type1 value1 | key2 type2 value2 | ... ]</pre>
   * Where the size is an unencoded int 32,
   * count is an encoded int32, the key is a string
   * and the value is stored in variant (type + value) format.
   */
  
  public static class SerializeMap extends StructureSerializer
  {
    @Override
    public void serializeContents(TupleWriter writer, FieldValue field) {
      MapValue map = field.getMap();
      writer.writeIntEncoded( map.size() );
      for ( String key : map.keys() ) {
        writer.writeString( key );
        writeVariant( writer, map.get( key ) );
      }
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
    
    // List serializers are not listed here because they must be selected
    // based on the type of the list elements.
    
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
    FieldSchema member = field.element();
    if ( member.type( ).isVariant() )
      return new SerializeVariantArray( );
    else if ( member.type( ) == DataType.LIST ) {
      return new SerializeArrayOfStructure( (StructureSerializer) serializerForSchema( member ) );
    } else if ( member.type( ) == DataType.MAP ) {
      return new SerializeArrayOfStructure( (StructureSerializer) getScalarSerializer( DataType.MAP ) );
    } else {
      return new SerializeScalarArray( serializerForSchema( member ) );
    }
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
