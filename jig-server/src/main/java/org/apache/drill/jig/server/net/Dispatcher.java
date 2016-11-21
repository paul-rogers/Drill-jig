package org.apache.drill.jig.server.net;

public interface Dispatcher
{
  void dispatch(RequestHandler.RequestContext request) throws Exception;    
}