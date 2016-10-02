package org.apache.drill.jig.drillpress.net;

import java.io.IOException;

import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.exception.JigIOException;
import org.apache.drill.jig.protocol.MessageConstants;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

public class MessageUtils
{
  public static ByteBuf headerBuf( ChannelHandlerContext ctx, int respCode, int length ) {
    ByteBuf header = Unpooled.buffer( MessageConstants.RESP_HEADER_LEN );
    header.writeShort( respCode );
    header.writeInt( length );
    return header;
  }
  
  public static void writeHeader( ChannelHandlerContext ctx, int respCode, int length ) {
    ctx.write( headerBuf( ctx, respCode, length ) );
  }

  public static <T> void write( ChannelHandlerContext ctx, int respCode, T resp, Schema<T> schema )
  {
    LinkedBuffer lbuf = LinkedBuffer.allocate( 1024 );
    byte bytes[] = ProtobufIOUtil.toByteArray( resp, schema, lbuf );
    ByteBuf respBuf = Unpooled.wrappedBuffer( bytes );
    writeHeader( ctx, respCode, bytes.length );
    ctx.writeAndFlush( respBuf );
  }

  public static <T> T read(RequestHandler.RequestContext request, Class<T> msgClass, Schema<T> schema) throws JigException {
    try {
      ByteBufInputStream in = new ByteBufInputStream( request.in );
      in.skipBytes( MessageConstants.REQ_HEADER_LEN );
      
      try {
        T msg = msgClass.newInstance();
        ProtobufIOUtil.mergeFrom( in, msg, schema);
        return msg;
      } catch (InstantiationException e) {
        throw new IllegalStateException( e );
      } catch (IllegalAccessException e) {
        throw new IllegalStateException( e );
      }
      finally {
        in.close( );
      }
    } catch (IOException e) {
      throw new JigIOException( e );
    }
  }

  static void emptyResponse(ChannelHandlerContext ctx, int respCode ) {
    writeHeader( ctx, respCode, 0 );
    ctx.flush();
  }
}
