package org.apache.drill.jig.serde.serializer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.drill.jig.serde.SerdeUtils;

public class TupleWriterV1 implements TupleWriter
{
  private ByteBuffer buf;

  @Override
  public void bind( ByteBuffer buf ) {
    this.buf = buf;
  }

  @Override
  public int startBlock( ) {
    int startPosn = buf.position( );
    buf.putInt( 0 ); // To be backpatched with length
    return startPosn;
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
    // Rotate the sign to the lowest-order bit,
    // Reverse the sign to map negative values (mostly 1's) to small
    // values (mostly 0's).
    
//    long high = n & 0x0400_0000_0000_0000L;
    int sign = (n<0) ? 1 : 0;
    n = (n<0) ? -n - 1 : n;
    n = n << 1 | sign;
    
    // 0xxx xxxx (1 byte)
    
    if ( (n & ~0x7FL) == 0 )
      buf.put( (byte) n );
    
    // 10xx xxxx | B (2 bytes)
    
    else if ( (n & ~0x3FFF) == 0 )
      buf.putShort( (short) ( 0x8000 | n ) );
    
    // 110x xxxx | BBB (4 bytes)
    
    else if ( (n & ~0x1FFF_FFFF) == 0 )
      buf.putInt( (int) ( 0xC000_0000 | n ) );
        
    // 1110 xxxx | BBB BBBB (8 bytes)
    
    else if ( (n & ~0x0FFF_FFFF_FFFF_FFFFL) == 0 ) {
      buf.putLong( 0xE000_0000_0000_0000L | n );
    }
    
    // 1111 000x | BBBB BBBB (9 bytes)
    
    else {
//      buf.put( (byte) (0xF0 & ((high == 0) ? 0 : 1)) );
      buf.put( (byte) 0xF0 );
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
  public void endBlock( int startPosn ) {
    int end = buf.position();
    int length = end - startPosn - 4;
    buf.putInt( startPosn, length );
  }

  @Override
  public void writeBytes(byte[] nullBits, int length) {
    // TODO Auto-generated method stub
    
  }
}