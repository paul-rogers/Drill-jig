package org.apache.drill.jig.drillpress;

import org.apache.drill.jig.protocol.MessageConstants;
import org.apache.drill.jig.direct.DrillClientContext;
import org.apache.drill.jig.direct.DrillConnectionFactory;
import org.apache.drill.jig.direct.DrillContextFactory;
import org.apache.drill.jig.direct.DrillSession;
import org.apache.drill.jig.direct.DrillSessionException;
import org.apache.drill.jig.drillpress.net.RequestProcessorFactory;

public class DrillPressContext
{
  public static final int SERVER_VERSION = 1;
  public static final int LOWEST_SUPPORTED_VERSION = 1;
  
  public enum DrillConnectMethod { EMBEDDED, DIRECT, ZK, CONFIG };
  
  private DrillConnectMethod connectMethod = DrillConnectMethod.CONFIG;
  private int drillPort;
  private String drillHost;
  private String zkConnectStr;
  private DrillClientContext drillClientContext;
  private int drillPressPort = MessageConstants.DEFAULT_PORT;
  private RequestProcessorFactory procFactory;
  private String userName;
  private String pwd;
  
  public DrillPressContext( ) {
    
  }
  
  public DrillPressContext onPort( int port ) {
    drillPressPort = port;
    return this;
  }
  
  public DrillPressContext withProcessor( RequestProcessorFactory procFactory ) {
    this.procFactory = procFactory;
    return this;
  }
  
  public DrillPressContext embedded( ) {
    connectMethod = DrillConnectMethod.EMBEDDED;
    return this;
  }
  
  public DrillPressContext direct( String host ) {
    drillHost = host;
    connectMethod = DrillConnectMethod.DIRECT;
    return this;
  }
  
  public DrillPressContext drillPort( int port ) {
    this.drillPort = port;
    return this;
  }
  
  public DrillPressContext viaZk( String connectStr ) {
    zkConnectStr = connectStr;
    connectMethod = DrillConnectMethod.ZK;
    return this;
  }
  
  public DrillPressContext viaConfig( ) {
    connectMethod = DrillConnectMethod.CONFIG;
    return this;
  }
  
  public void init( ) {
    if ( drillClientContext != null )
      return;
    DrillContextFactory factory = new DrillContextFactory( );
    if ( connectMethod == DrillConnectMethod.EMBEDDED ) {
      factory.withEmbeddedDrillbit();
    }
    drillClientContext = factory.build( );
  }
  
  public void shutDown( ) {
    if ( drillClientContext != null ) {
      try {
        drillClientContext.close();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  public void withLogin( String userName, String pwd ) {
    this.userName = userName;
    this.pwd = pwd;
  }
  
  public DrillSession connectToDrill( ) throws DrillSessionException {
    DrillConnectionFactory factory = new DrillConnectionFactory( );
    switch ( connectMethod ) {
    case CONFIG:
      factory.viaZk();
      break;
    case DIRECT:
      if ( drillPort == 0 ) {
        factory.drillbit( drillHost );
      } else {
        factory.drillbit( drillHost, drillPort );
      }
      break;
    case EMBEDDED:
      factory.embedded();
      break;
    case ZK:
      factory.viaZk( zkConnectStr );
      break;
    default:
      throw new IllegalStateException( "Unknown connect method: " + connectMethod );   
    }
    if ( userName != null )
      factory.withLogin( userName, pwd );
    return factory.connect( );
  }
  
  public int getDrillPressPort( ) {
    return drillPressPort;
  }
  
  public RequestProcessorFactory getProcessor( ) {
    if ( procFactory == null ) {
      procFactory = new SessionProcessorFactory( this );
    }
    return procFactory;
  }
}
