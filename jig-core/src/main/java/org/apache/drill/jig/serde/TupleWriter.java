package org.apache.drill.jig.serde;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public interface TupleWriter
{
  void startBlock(ByteBuffer buf);

  void writeHeader(boolean isNull[], boolean isRepeated[]);

  void writeString(String value);

  void writeBoolean(boolean value);
  void writeByte(byte b);
  void writeShort(short value);
  void writeIntEncoded( int n );
  void writeLongEncoded( long n );
//  void writeInt(int value);
  void writeLong(long value);
  
  void writeFloat(float value);
  void writeDouble(double value);

  void writeDecimal(BigDecimal value);

  void endBlock();
}