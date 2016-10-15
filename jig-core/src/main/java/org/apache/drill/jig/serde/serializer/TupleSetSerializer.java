package org.apache.drill.jig.serde.serializer;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.serde.BaseTupleSetSerde;
import org.apache.drill.jig.serde.SerdeUtils;
import org.apache.drill.jig.api.TupleSchema;

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
   * An Any type is special: the serialized form includes the type, then the
   * value serialized in the format for that type.
   */
  
  public static class SerializeVariant implements FieldSerializer
  {
    private FieldSerializer[] serializers;
    
    public SerializeVariant( FieldSerializer serializers[] ) {
      this.serializers = serializers;
    }
    
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      DataType type = field.type();
      writer.writeByte( (byte) type.typeCode( ) );
      serializers[ type.ordinal() ].serialize( writer, field );
    }   
  }
  
  public static class SerializeArray implements FieldSerializer
  {
    private FieldSerializer[] serializers;
    
    public SerializeArray( FieldSerializer serializers[] ) {
      this.serializers = serializers;
    }  
    
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      int startPosn = writer.startBlock( );
      ArrayValue array = field.getArray();
      int n = array.size( );
      writer.writeIntEncoded( n );
      if ( array.memberIsNullable( ) ) {
        byte nullBits[] = new byte[(n+7)/8];
        int posn = 0;
        for ( int i = 0;  i < nullBits.length;  i++ ) {
          int flags = 0;
          for ( int j = 0;  j < 8;  j++ ) {
            flags <<= 1;
            if ( array.get(posn++).isNull())
              flags |= 1;
          }
          nullBits[i] = (byte) flags;
        }
        writer.writeBytes(nullBits, nullBits.length);
      }
      FieldSerializer fieldSerializer = serializers[ array.memberType().ordinal() ];
      for ( int i = 0;  i < n;  i++ ) {
        FieldValue member = array.get(i);
        if ( ! member.isNull() )
          fieldSerializer.serialize( writer, member );
      }
      writer.endBlock( startPosn );
    }
  }
  
  public static class SerializeMap implements FieldSerializer
  {
    private FieldSerializer[] serializers;
    
    public SerializeMap( FieldSerializer serializers[] ) {
      this.serializers = serializers;
    }  
    
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      int startPosn = writer.startBlock( );
      MapValue map = field.getMap();
      writer.writeIntEncoded( map.size() );
      for ( String key : map.keys() ) {
        writer.writeString( key );
        FieldValue value = map.get( key );
        serializers[value.type().ordinal()].serialize(writer, value);
      }
      writer.endBlock( startPosn );
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
    serializers[ DataType.VARIANT.ordinal() ] = new SerializeVariant( serializers );
    serializers[ DataType.LIST.ordinal() ] = new SerializeArray( serializers );
    serializers[ DataType.MAP.ordinal() ] = new SerializeMap( serializers );
    return serializers;
  }

  /**
   * Set up various tables used to speed serialization.
   */
  
  private void prepare( ) {
    super.prepare( schema.count( ) );
    serializer = new FieldSerializer[ fieldCount ];
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldSchema field = schema.field( i );
      serializer[i] = serializerByType[ field.type().ordinal() ];
    }
  }
  
  /**
   * Write the schema to the buffer as a length, a field count and
   * a (name, type, cardinality) triple for each field.
   * 
   * @param buf
   */
  
  public void serializeSchema( ByteBuffer buf ) {
    writer.bind( buf );
    int startPosn = writer.startBlock( );
    writer.writeIntEncoded( fieldCount );
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldSchema field = schema.field( i );
      writer.writeString( field.name() );
      writer.writeByte( (byte) field.type().typeCode( ) );
      writer.writeByte( (byte) SerdeUtils.encode( field.nullable( ) ) );
    }
    writer.endBlock( startPosn );
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
}
