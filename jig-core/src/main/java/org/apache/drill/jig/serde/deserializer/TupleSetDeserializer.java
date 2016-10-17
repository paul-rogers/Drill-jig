package org.apache.drill.jig.serde.deserializer;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.serde.BaseTupleSetSerde;

public class TupleSetDeserializer extends BaseTupleSetSerde
{
  protected TupleReader reader = new TupleReaderV1( );
  private final int fieldTypeCodes[];
  protected final int fieldIndexes[];
  
  public TupleSetDeserializer( TupleSchema schema ) {
    super( schema );
    fieldTypeCodes = new int[ fieldCount ];
    fieldIndexes = new int[ fieldCount ];
  }
  
  /**
   * Tuples are deserialized in two parts. This method reads the header and
   * builds indexes to the start of each (non-null) field. Then, the field
   * accessors pull out the field data on demand. Because of this random
   * access pattern, the caller must call {@link #endTuple} before moving
   * to the next tuple in the same buffer.
   */
  
  public boolean startTuple( ByteBuffer buf ) {
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
    else if ( fieldLen == org.apache.drill.jig.api.Constants.BLOCK_LENGTH_AND_VALUE ) {
      fieldLen = reader.readInt();
      reader.skip( fieldLen );
    }
    else if ( fieldLen == org.apache.drill.jig.api.Constants.NOT_IMPLEMENTED )
      throw new IllegalStateException( "Unsupported type: " + type );
    else if ( fieldLen == org.apache.drill.jig.api.Constants.TYPE_AND_VALUE ) {
      skipFieldOfType( reader.readByte() );
    }
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
