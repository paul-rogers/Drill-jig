package org.apache.drill.jig.serde.deserializer;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.serde.BaseTupleSetSerde;

public class TupleSetDeserializer extends BaseTupleSetSerde
{
  protected class DeserializedTupleAccessor implements TupleValue
  {
    @Override
    public TupleSchema schema() {
      return schema;
    }

    @Override
    public FieldValue field(int i) {
      if ( i < 0  ||  i >= accessors.length )
        return null;
      return accessors[i];
    }

    @Override
    public FieldValue field(String name) {
      FieldSchema field = schema.field( name );
      if ( field == null )
        return null;
      return accessors[ field.index() ];
    }    
  }
    
  protected TupleReader reader = new TupleReaderV1( );
  private int fieldTypeCodes[];
  protected int fieldIndexes[];
  protected DeserializedTupleAccessor tuple = new DeserializedTupleAccessor( );
  private FieldValue accessors[];
    
//  public void deserializeAndPrepareSchema( ByteBuffer buf ) {
//    TupleSchemaImpl schemaImpl = new TupleSchemaImpl( );
//    schema = schemaImpl;
//    reader.bind( buf );
//    reader.startBlock( );
//    fieldCount = reader.readIntEncoded( );
//    fieldTypeCodes = new int[ fieldCount ];
//    accessors = new FieldValue[ fieldCount ];
//    for ( int i = 0;  i < fieldCount;  i++ ) {
//      String name = reader.readString( );
//      fieldTypeCodes[i] = reader.readByte( );
//      DataType type = DataType.typeForCode( fieldTypeCodes[i] );
//      FieldSchemaImpl field = new FieldSchemaImpl( name, type, SerdeUtils.decode( reader.readByte() ) );
//      schemaImpl.add( field );
//      assert false;
//      // TODO: Fix this
////      BufferFieldAccessor accessor = BufferFieldAccessor.makeAccessor( field );
////      accessor.bind( this, i );
////      accessors[i] = accessor;
//    }
//    prepare( fieldCount );
//    fieldIndexes = new int[ fieldCount ];
//  }
  
//  public void deserializeSchema( ByteBuffer buf ) {
//    TupleSchemaImpl schemaImpl = new TupleSchemaImpl( );
//    schema = schemaImpl;
//    reader.startBlock( buf );
//    fieldCount = reader.readIntEncoded( );
//    for ( int i = 0;  i < fieldCount;  i++ ) {
//      String name = reader.readString( );
//      DataType type = DataType.typeForCode( reader.readByte( ) );
//      FieldSchemaImpl field = new FieldSchemaImpl( name, type, SerdeUtils.decode( reader.readByte() ) );
//      schemaImpl.add( field );
//    }
//  }
  
  public void prepareSchema( TupleSchema schema ) {
    this.schema = schema;
    fieldCount = schema.count();
    fieldTypeCodes = new int[ fieldCount ];
    accessors = new FieldValue[ fieldCount ];
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldSchema field = schema.field( i );
      fieldTypeCodes[i] = field.type().typeCode();
      assert false;
      // TODO: Fix this
//      BufferFieldAccessor accessor = BufferFieldAccessor.makeAccessor( field );
//      accessor.bind( this, i );
//      accessors[i] = accessor;
    }
    prepare( fieldCount );
    fieldIndexes = new int[ fieldCount ];
  }
  
  /**
   * Tuples are deserialized in two parts. This method reads the header and
   * builds indexes to the start of each (non-null) field. Then, the field
   * accessors pull out the field data on demand. Because of this random
   * access pattern, the caller must call {@link #endTuple} before moving
   * to the next tuple in the same buffer.
   */
  
  public boolean deserializeTuple( ByteBuffer buf ) {
    reader.bind( buf );
    if ( ! reader.startBlock( ) )
      return false;
    reader.readHeader( isNull, isRepeated );
    buildFieldIndex( );
    return true;
  }
  
  public void endTuple( ) {
    reader.endBlock();
  }
  
  private void buildFieldIndex( ) {
    for ( int i = 0;  i < fieldCount;  i++ ) {
      int index = -1;
      if ( ! isNull[i] ) {
        index = reader.position( );
        skipField( i );
      }
      fieldIndexes[i] = index;
    }
  }

  private void skipField(int i) {
    skipFieldOfType( fieldTypeCodes[i] );
  }
  
  private void skipFieldOfType( int type )
  {
    int fieldLen = DataType.lengthForCode[ type ];
    if ( fieldLen > 0 ) {
      reader.skip( fieldLen );
    }
    else if ( fieldLen == org.apache.drill.jig.api.Constants.ENCODED_LONG ) {
      // Easiest way to skip a number is to read it.
      reader.readLongEncoded();
    }
    else if ( fieldLen == org.apache.drill.jig.api.Constants.LENGTH_AND_VALUE ) {
      fieldLen = reader.readIntEncoded();
      reader.skip( fieldLen );
    }
    else if ( fieldLen == org.apache.drill.jig.api.Constants.TYPE_AND_VALUE ) {
      skipFieldOfType( reader.readByte() );
    }
  }

  public TupleSchema getSchema() {
    return schema;
  }

  public TupleValue getTuple() {
    return tuple;
  }
  
  public TupleReader reader( ) {
    return reader;
  }
  
  public boolean isNull( int index ) {
    return isNull[ index ];
  }
  
  public void seek( int index ) {
    reader.seek( fieldIndexes[ index ] );
  }
  
  public void seekVariant( int index ) {
    reader.seek( fieldIndexes[ index ] + 1 );
  }
}
