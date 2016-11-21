package org.apache.drill.jig.server.net;

import org.apache.drill.jig.exception.JigException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public interface RequestHandler
{
  class RequestContext
  {
    int opCode;
    int length;
    ByteBuf in;
    ChannelHandlerContext ctx;
    
    public RequestContext(int opCode, int length, ByteBuf in, ChannelHandlerContext ctx) {
      this.opCode = opCode;
      this.length = length;
      this.in = in;
      this.ctx = ctx;
    }
  }

  void hello( RequestHandler.RequestContext request ) throws JigException;
  void listLogins( RequestHandler.RequestContext request ) throws JigException;
  void loginProperties( RequestHandler.RequestContext request ) throws JigException;
  void login( RequestHandler.RequestContext request ) throws JigException;
  
  void executeStmt( RequestHandler.RequestContext request ) throws JigException;
  void executeQuery( RequestHandler.RequestContext request ) throws JigException;
  void requestData( RequestHandler.RequestContext request ) throws JigException;
  void cancelQuery(RequestHandler.RequestContext request) throws JigException;
  
  void goodbye( RequestHandler.RequestContext request ) throws JigException;
}