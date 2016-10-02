package org.apache.drill.jig.serde;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class TupleWriterV1 implements TupleWriter
{
  private ByteBuffer buf;
  private int startPosn;

  @Override
  public void startBlock( ByteBuffer buf ) {
    this.buf = buf;
    startPosn = buf.position( );
    buf.putInt( 0 ); // To be backpatched with length
  }
  
  @Override
  public void writeHeader( boolean isNull[], boolean isRepeated[] ) {
    assert (isNull.length % 4) == 0;
    assert isNull.length == isRepeated.length;
    int byteCount = isNull.length / 4;
    int posn = 0;
    for ( int i = 0;  i < byteCount;  i++ ) {
      int value = SerdeUtils.encode( isNull[posn] ) * 2 + SerdeUtils.encode( isRepeated[posn++] );
      value = (value << 2) + SerdeUtils.encode( isNull[posn] ) * 2 + SerdeUtils.encode( isRepeated[posn++] );
      value = (value << 2) + SerdeUtils.encode( isNull[posn] ) * 2 + SerdeUtils.encode( isRepeated[posn++] );
      value = (value << 2) + SerdeUtils.encode( isNull[posn] ) * 2 + SerdeUtils.encode( isRepeated[posn++] );
      buf.put( (byte) value );
    }
  }
  
  @Override
  public void writeLongEncoded( long n )
  {
    // 0xxx xxxx (1 byte)
    
    if ( n < 0x7F )
      buf.put( (byte) n );
    
    // 10xx xxxx | B (2 bytes)
    
    else if ( n < 0x3FFF )
      buf.putShort( (short) ( 0x8000 | n ) );
    
    // 110x xxxx | BBB (4 bytes)
    
    else if ( n < 0x1FFF_FFFF )
      buf.putInt( (int) ( 0xC000_0000 | n ) );
    
    // 1110 0000 | BBBB BBBB (9 bytes)
    
    else {
      buf.put( (byte) 0xE0 );
      buf.putLong( n );
    }
  }

  @Override
  public void writeByte(byte b) { buf.put( b ); }

  @Override
  public void writeShort(short value) { buf.putShort( value ); }

  @Override
  public void writeIntEncoded(int value) { writeLongEncoded( value ); }

  @Override
  public void writeString(String value) {
    byte encoded[] = value.getBytes( org.apache.drill.jig.api.Constants.utf8Charset );
    writeIntEncoded( encoded.length );
    buf.put( encoded );
  }

  @Override
  public void writeBoolean(boolean value) {
    buf.put( SerdeUtils.encode( value ) );
  }

  @Override
  public void writeLong(long value) { buf.putLong( value ); }
  
  @Override
  public void writeFloat(float value) { buf.putFloat( value ); }  
 
  @Override
  public void writeDouble(double value) { buf.putDouble( value ); }  

  @Override
  public void writeDecimal(BigDecimal value) {
    writeString( value.toString() );
  }

  @Override
  public void endBlock( ) {
    int end = buf.position();
    int length = end - startPosn - 4;
    buf.putInt( startPosn, length );
  }
}