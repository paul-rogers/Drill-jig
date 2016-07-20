package org.apache.drill.jig.serde;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class TupleReaderV1 implements TupleReader
{
  private ByteBuffer buf;
  private int blockStartPosn;
  private int blockLen;

  @Override
  public boolean startBlock( ByteBuffer buf ) {
    this.buf = buf;
    if ( buf.position( ) == buf.limit() )
      return false;
    blockLen = buf.getInt();
    blockStartPosn = buf.position();
    return true;
  }
  
  @Override
  public void readHeader( boolean isNull[], boolean isRepeated[] ) {
    assert (isNull.length % 4) == 0;
    assert isNull.length == isRepeated.length;
    int byteCount = isNull.length / 4;
    int posn = 0;
    for ( int i = 0;  i < byteCount;  i++ ) {
      byte b = buf.get();
      isNull[posn] = Constants.decode(b & 0x80);
      isRepeated[posn++] = Constants.decode(b & 0x40);
      isNull[posn] = Constants.decode(b & 0x20);
      isRepeated[posn++] = Constants.decode(b & 0x10);
      isNull[posn] = Constants.decode(b & 0x08);
      isRepeated[posn++] = Constants.decode(b & 0x04);
      isNull[posn] = Constants.decode(b & 0x02);
      isRepeated[posn++] = Constants.decode(b & 0x01);
    }
  }
  
  @Override
  public boolean readBoolean( ) {
    return Constants.decode( buf.get( ) );
  }
  
  @Override
  public byte readByte( ) { return buf.get(); }
  
  @Override
  public short readShort( ) { return buf.getShort(); }
  
  @Override
  public long readLongEncoded( )
  {
    byte b = buf.get( );
    if ( (b & 0x80) == 0 ) {
      // 0xxx xxxx (1 byte)
      return (b & 0x7F);
    }
    
    if ( (b & 0xE0 ) == 0xE0 ) {
      
      // 1110 0000 | BBBB BBBB (9 bytes)
      
      return buf.getLong();
    }
    
    buf.position( buf.position() - 1 );
    if ( (b & 0xC0) == 0x80 ) {

      // 10xx xxxx | B (2 bytes)
    
      return buf.getShort() & 0x3FFF;
    }
    
    // 110x xxxx | BBB (4 bytes)
    
    return buf.getInt() & 0x1FFF_FFFF;
  }
  
  @Override
  public int readIntEncoded( ) {
    return (int) readLongEncoded( );
  }

  @Override
  public String readString( ) {
    int len = readIntEncoded( );
    String value = new String( buf.array(), buf.position(), len,
        org.apache.drill.jig.api.Constants.utf8Charset );
    skip( len );
    return value;
  }

  @Override
  public long readLong( ) { return buf.getLong( ); }
  
  @Override
  public float readFloat( ) { return buf.getFloat( ); }  
 
  @Override
  public double readDouble( ) { return buf.getDouble( ); }  

  @Override
  public BigDecimal readDecimal( ) {
    String value = readString( );
    return new BigDecimal( value );
  }

  @Override
  public void endBlock( ) {
    buf.position( blockStartPosn + blockLen );
  }

  @Override
  public void skip(int len) {
    buf.position( buf.position() + len );
  }

  @Override
  public int position() {
    return buf.position();
  }

  @Override
  public void seek(int posn) {
    buf.position( posn );
  }
}
