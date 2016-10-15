package org.apache.drill.jig.serde;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.apache.drill.jig.serde.deserializer.TupleReaderV1;
import org.apache.drill.jig.serde.serializer.TupleWriterV1;
import org.junit.Test;

public class ReaderWriterTest {

  @Test
  public void test() {
    ByteBuffer buf = ByteBuffer.allocate( 1024 );
    doTest( buf, 0, 1 );
    doTest( buf, 1, 1 );
    doTest( buf, -1, 1 );
    doTest( buf, 0x3F, 1 );
    doTest( buf, -0x3F, 1 );
    doTest( buf, 0x40, 2 );
    doTest( buf, -0x40, 1 );
    doTest( buf, -0x41, 2 );
    doTest( buf, 0x7f, 2 );
    doTest( buf, -0x7f, 2 );
    doTest( buf, 0x1FFF, 2 );
    doTest( buf, -0x1FFF, 2 );
    doTest( buf, 0x2000, 4 );
    doTest( buf, -0x2000, 2 );
    doTest( buf, -0x2001, 4 );
    doTest( buf, 0x0800_0000, 4 );
    doTest( buf, -0x0800_0000, 4 );
    doTest( buf, 0x0FFF_FFFF, 4 );
    doTest( buf, -0x0FFF_FFFF, 4 );
    doTest( buf, 0x1000_0000, 8 );
    doTest( buf, -0x1000_0000, 4 );
    doTest( buf, -0x1000_0001, 8 );
    doTest( buf, 0x07FF_FFFF_FFFF_FFFFL, 8 );
    doTest( buf, -0x07FF_FFFF_FFFF_FFFFL, 8 );
    doTest( buf, 0x0800_0000_0000_0000L, 9 );
    doTest( buf, -0x0800_0000_0000_0000L, 8 );
    doTest( buf, -0x0800_0000_0000_0001L, 9 );
    doTest( buf, Long.MAX_VALUE, 9 );
    doTest( buf, Long.MIN_VALUE, 9 );
  }
  
  private void doTest( ByteBuffer buf, long value, int len ) {
    buf.clear();
    TupleWriterV1 writer = new TupleWriterV1( );
    writer.bind( buf );
    int start = buf.position();
    writer.writeLongEncoded( value );
    assertEquals( start+len, buf.position() );
    buf.flip();
    TupleReaderV1 reader = new TupleReaderV1( );
    reader.bind( buf );
    assertEquals( value, reader.readLongEncoded() );
  }

}
