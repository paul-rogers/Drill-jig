package org.apache.drill.jig.drillpress.net;

public interface Dispatcher
{
  void dispatch(RequestHandler.RequestContext request) throws Exception;    
}