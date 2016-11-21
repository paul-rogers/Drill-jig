package org.apache.drill.jig.server;

import java.util.LinkedList;
import java.util.List;

import org.apache.drill.jig.server.net.RequestProcessor;
import org.apache.drill.jig.server.net.RequestProcessorFactory;

public class SessionProcessorFactory implements RequestProcessorFactory
{
  private DrillPressContext drillPressContext;
  private List<SessionRequestProcessor> sessions = new LinkedList<>( );
  
  public SessionProcessorFactory(DrillPressContext drillPressContext) {
    this.drillPressContext = drillPressContext;
  }

  @Override
  public synchronized RequestProcessor newProcessor() {
    SessionRequestProcessor session = new SessionRequestProcessor( drillPressContext );
    sessions.add( session );
    return session;
  }
  
  public synchronized void sessionEnded( SessionRequestProcessor session ) {
    sessions.remove( session );
  }

}
