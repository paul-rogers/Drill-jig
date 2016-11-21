package org.apache.drill.jig.server.net;

import java.nio.ByteOrder;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class JigFrameDecoder extends LengthFieldBasedFrameDecoder
{
  public JigFrameDecoder( ) {
    super (  ByteOrder.BIG_ENDIAN,
      Integer.MAX_VALUE, // maxFrameLength
      2, // lengthFieldOffset
      4, // lengthFieldLength
      0, // lengthAdjustment
      0, // initialBytesToStrip
      false ); // failFast
  }
}