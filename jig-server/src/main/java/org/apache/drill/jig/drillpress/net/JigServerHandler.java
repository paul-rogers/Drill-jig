package org.apache.drill.jig.drillpress.net;

import java.io.IOException;

import org.apache.drill.jig.protocol.MessageConstants;
import org.apache.drill.jig.proto.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class JigServerHandler extends ChannelInboundHandlerAdapter
{
  private static final Logger logger = LoggerFactory.getLogger(JigServerHandler.class);
  Dispatcher dispatcher;
  
  public JigServerHandler( Dispatcher dispatcher ) {
    this.dispatcher = dispatcher;   
  }
  
  public JigServerHandler( RequestProcessor requestProcessor ) {
    dispatcher = new RequestDispatcher( 
        new RequestHandlerImpl( requestProcessor ) );

  }
  
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ByteBuf in = (ByteBuf) msg;
      int opCode = in.getShort( 0 );
      int length = in.getInt( 2 );
      RequestHandler.RequestContext request = new RequestHandler.RequestContext( opCode, length, in, ctx );
      try {
        dispatcher.dispatch( request );
      }
      catch ( RequestException e ) {
        ErrorResponse resp = new ErrorResponse( )
            .setCode( e.errorCode )
            .setSqlCode( e.sqlCode )
            .setMessage( e.getMessage() );
        MessageUtils.write(ctx, MessageConstants.ERROR_RESP, resp, ErrorResponse.getSchema() );
      }
      catch ( IOException e ) {
        logger.error( "Request: " + opCode + " raised an I/O exception, closing connection:", e );
        ctx.close( );
      }
      catch ( Exception e ) {
        logger.error( "Request: " + opCode + " raised an exception, closing connection:", e );
        ctx.close( );
      }
      finally {
        in.release();
      }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx,
   Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}