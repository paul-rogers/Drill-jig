package org.apache.drill.jig.serde;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public interface TupleReader
{
  void bind(ByteBuffer buf);
  boolean startBlock( );

  void readHeader(boolean isNull[], boolean isRepeated[]);

  boolean readBoolean();
  byte readByte( );
  short readShort( );
  int readIntEncoded();
  long readLongEncoded();
  long readLong( );

  float readFloat();
  double readDouble();

  BigDecimal readDecimal();
  
  String readString();

  void endBlock();

  void skip( int len );
  
  int position();

  void seek(int posn);

}