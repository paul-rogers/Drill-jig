package org.apache.drill.jig.server;

import org.apache.drill.jig.server.DrillPressContext.DrillConnectMethod;
import org.apache.drill.jig.util.JigUtilities;

import com.typesafe.config.Config;

/**
 * Main program for the Drillpress server. Jig is modular. The Jig server
 * (AKA Drillpress) is packaged as an embedded service. This main program
 * simply acts as the container holding the embedded Drillpress. The
 * embedded Drillpress itself is just a messaging wrapper around the Jig
 * direct implementation, which is, in turn, a wrapper around a Drill
 * client.
 * <p>
 * The primary tasks are:
 * <ul>
 * <li>Retrieve configuration from the Drillpress configuration files
 * and pass it to the embedded Drillpress. (The embedded Drillpress takes
 * its configuration via Java methods.)</li>
 * <li>Start the Drillpress server.</li>
 * <li>Listen for shutdown events and gracefully shut down the Drillpress
 * server in response.</li>
 * </ul>
 */

public class DrillPress
{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillPress.class);

  private EmbeddedDrillPress drillPress;
  
  public static void main( String args[] ) {
    
    // Command-line arguments are not used.
    
    try {
      new DrillPress( ).run( );
    } catch (DrillPressException e) {
      System.err.println( "Drillpress run failed " );
      e.printStackTrace( System.err );
      System.exit( -1 );
    }
  }

  private void run() throws DrillPressException {
    registerSignalHandler( );
    loadConfig( );
    DrillPressContext context = buildDrillContext( );
    runDrillpress( context );
  }

  /**
   * Catches shutdown events and asks the Drill press server
   * to exit. Upon successful termination, the Drill press thread
   * exits, causing the last Java thread to exit, allowing the
   * JVM to exit.
   */
  
  private void registerSignalHandler() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (drillPress != null) {
          logger.info( "Shut down signal received, shutting down Drillpress." );
          drillPress.stop();
        }
      }
    });
  }

  private void loadConfig() {
    DrillPressConfig.init( );
  }

  private DrillPressContext buildDrillContext() throws DrillPressException {
    Config config = DrillPressConfig.config();
    DrillPressContext context = new DrillPressContext( );
    DrillConnectMethod drillMethod = DrillConnectMethod.CONFIG;
    String drillMethodStr = config.getString( DrillPressConfig.DRILL_CONNECT_METHOD );
    if ( ! JigUtilities.isBlank( drillMethodStr ) ) {
      DrillConnectMethod decoded = DrillConnectMethod.valueOf( drillMethodStr.toUpperCase() );
      if ( decoded == null )
        throw new DrillPressException( DrillPressConfig.DRILL_CONNECT_METHOD +
            " does not name a valid Drill connect method: " + drillMethodStr );
    }
    
    logger.info( "Drill connect method: " + drillMethod );
    switch ( drillMethod ) {
    case CONFIG:
      context.viaConfig();
      break;
    case DIRECT:
      context.direct( config.getString( DrillPressConfig.DRILL_HOST ) );
      int port = config.getInt( DrillPressConfig.DRILL_USER_PORT );
      if ( port != 0 )
        context.drillPort( port );
      break;
    case EMBEDDED:
      context.embedded();
      break;
    case ZK:
      context.viaZk( config.getString( DrillPressConfig.ZK_CONNECT ) );
      break;
    default:
      throw new IllegalArgumentException( "Unknown method: " + drillMethod );
    }
    
    // Drill press listen port
    
    int dbPort = config.getInt( DrillPressConfig.DRILLPRESS_PORT );
    if ( dbPort != 0 )
      context.onPort( dbPort );
    return context;
  }

  /**
   * Start the Drill Press server. The server runs on a separate
   * thread, allowing this thread to exit, but keeping the JVM
   * alive.
   * 
   * @param context
   */
  
  private void runDrillpress( DrillPressContext context ) {
    drillPress = new EmbeddedDrillPress( context );
    
    logger.info( "Starting Drillpress server." );
    drillPress.start( );
  }
}
