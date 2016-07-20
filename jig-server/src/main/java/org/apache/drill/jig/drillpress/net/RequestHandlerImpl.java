package org.apache.drill.jig.drillpress.net;

import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.protocol.MessageConstants;
import org.apache.drill.jig.proto.ExecuteRequest;
import org.apache.drill.jig.proto.InformationResponse;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesRequest;
import org.apache.drill.jig.proto.LoginPropertiesResponse;
import org.apache.drill.jig.proto.LoginRequest;
import org.apache.drill.jig.proto.QueryRequest;
import org.apache.drill.jig.proto.SchemaResponse;
import org.apache.drill.jig.proto.SuccessResponse;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.protocol.HelloRequest;
import org.apache.drill.jig.protocol.HelloResponse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

public class RequestHandlerImpl implements RequestHandler
{
  RequestProcessor processor;
  
  public RequestHandlerImpl( RequestProcessor processor ) {
    this.processor = processor;   
  }
  
  @Override
  public void hello(RequestHandler.RequestContext request) throws JigException {
    if ( request.length != MessageConstants.HELLO_REQ_LEN )
      throw new IllegalArgumentException( "Hello message of wrong length: " + request.length );
    ByteBuf reqBuf = request.in;
    reqBuf.skipBytes( MessageConstants.REQ_HEADER_LEN );
    HelloRequest req = new HelloRequest( reqBuf.readShort(), reqBuf.readShort() );
    HelloResponse resp = processor.hello( req );
    MessageUtils.writeHeader( request.ctx, MessageConstants.HELLO_RESP, MessageConstants.HELLO_RESP_LEN );
    ByteBuf respBuf = Unpooled.buffer( MessageConstants.HELLO_RESP_LEN );
    respBuf.writeShort( resp.serverVersion );
    respBuf.writeShort( resp.sessionVersion );
    request.ctx.writeAndFlush( respBuf );
  }

  @Override
  public void listLogins(RequestHandler.RequestContext request) throws JigException {
    ListLoginsResponse resp = processor.listLogins( );
    MessageUtils.write( request.ctx, MessageConstants.LIST_LOGIN_METHODS_RESP, resp, ListLoginsResponse.getSchema() );
  }
  
  @Override
  public void loginProperties(RequestHandler.RequestContext request) throws JigException {
    LoginPropertiesRequest req = MessageUtils.read( request, LoginPropertiesRequest.class, LoginPropertiesRequest.getSchema( ) );
    LoginPropertiesResponse resp = processor.loginProperties( req );
    MessageUtils.write( request.ctx, MessageConstants.LOGIN_PROPS_RESP, resp, LoginPropertiesResponse.getSchema() );
  }

  @Override
  public void login(RequestHandler.RequestContext request) throws JigException {
    LoginRequest req = MessageUtils.read( request, LoginRequest.class, LoginRequest.getSchema( ) );
    processor.login( req );
    MessageUtils.emptyResponse( request.ctx, MessageConstants.OK_RESP );
  }

  @Override
  public void executeStmt(RequestHandler.RequestContext request) throws JigException {
    ExecuteRequest req = MessageUtils.read( request, ExecuteRequest.class, ExecuteRequest.getSchema( ) );
    SuccessResponse resp = processor.executeStmt( req );
    MessageUtils.write( request.ctx, MessageConstants.SUCCESS_RESP, resp, SuccessResponse.getSchema() );
  }

  @Override
  public void executeQuery(RequestHandler.RequestContext request) throws JigException {
    QueryRequest req = MessageUtils.read( request, QueryRequest.class, QueryRequest.getSchema( ) );
    processor.executeQuery( req );
    MessageUtils.emptyResponse( request.ctx, MessageConstants.OK_RESP );
  }
  
  @Override
  public void requestData(RequestHandler.RequestContext request) throws JigException {
    DataResponse results = processor.requestData();
    switch ( results.type ) {
    case NO_DATA:
      MessageUtils.write( request.ctx, MessageConstants.INFO_RESP, results.info, InformationResponse.getSchema() );
      break;
    case SCHEMA:
      MessageUtils.write( request.ctx, MessageConstants.SCHEMA_RESP, results.schema, SchemaResponse.getSchema() );
      break;
    case DATA:
      if ( results.buf != null )
      {
        MessageUtils.writeHeader( request.ctx, MessageConstants.RESULTS_RESP, results.buf.position( ) );
        request.ctx.writeAndFlush( Unpooled.wrappedBuffer( results.buf.array(), 0, results.buf.position() ) );
      } else {
        MessageUtils.writeHeader( request.ctx, MessageConstants.RESULTS_RESP, results.data.length );
        request.ctx.writeAndFlush( Unpooled.wrappedBuffer( results.data ) );
      }
      break;
    case EOF:
      MessageUtils.emptyResponse( request.ctx, MessageConstants.EOF_RESP );
      break;
    }    
  }

  @Override
  public void cancelQuery(RequestHandler.RequestContext request) throws JigException {
    processor.cancelQuery( );
    MessageUtils.emptyResponse( request.ctx, MessageConstants.OK_RESP );
  }

  @Override
  public void goodbye(RequestHandler.RequestContext request) throws JigException {
    processor.goodbye( );
    MessageUtils.writeHeader( request.ctx, MessageConstants.GOODBYE_RESP, 0 );
    request.ctx.writeAndFlush( request ).addListener(
        new ChannelFutureListener( ) {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            future.channel().close();
            future.channel( ).parent( ).close( );
          }
    });
  } 
}