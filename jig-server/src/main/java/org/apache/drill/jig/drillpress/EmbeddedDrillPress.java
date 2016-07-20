package org.apache.drill.jig.drillpress;

import java.util.concurrent.CountDownLatch;

import org.apache.drill.jig.drillpress.net.DrillPressServer;
import org.apache.drill.jig.drillpress.net.DrillPressServer.ReadyListener;

public class EmbeddedDrillPress
{
  private class EmbeddedDrillPressThread extends Thread implements ReadyListener
  {
    private CountDownLatch readyLatch = new CountDownLatch( 1 );
    private CountDownLatch stopLatch = new CountDownLatch( 1 );
    private DrillPressServer server;
    
    public EmbeddedDrillPressThread(  ) {
      super( "Embedded DrillPress" );
    }
    
    @Override
    public void run( )
    {
      try {
        runServer( );
      } catch (InterruptedException e) {
        System.err.println( "Interrupted" );
        e.printStackTrace();
      }
    }

    private void runServer() throws InterruptedException {
      server = new DrillPressServer( context );
      server.setReadyListener( this );
      server.start();
      stopLatch.await();
      server.stop( );
    }

    @Override
    public void ready() {
      readyLatch.countDown();
    }
    
    public void shutDown( ) {
      stopLatch.countDown();
    }
  }    

  private EmbeddedDrillPressThread thread;
  private DrillPressContext context;
  
  public EmbeddedDrillPress( DrillPressContext context ) {
    this.context = context;
  }
  
  public EmbeddedDrillPress start( ) {
    assert thread == null;
    if ( thread != null ) {
      throw new IllegalStateException( "Embedded DrillPress already started" );
    }
    thread = new EmbeddedDrillPressThread( );
    thread.start();
    try {
      thread.readyLatch.await();
    } catch (InterruptedException e) {
      // Should not happen
    }
    return this;
  }
  
  public void shutDown( ) {
    assert thread != null;
    if ( thread == null )
      return;
    thread.shutDown( );
  }
  
  public void join( ) {
    try {
      thread.join();
    } catch (InterruptedException e) {
      // Should not occur
    }
  }
  
  public void stop( ) {
    shutDown( );
    join( );
  }
}
