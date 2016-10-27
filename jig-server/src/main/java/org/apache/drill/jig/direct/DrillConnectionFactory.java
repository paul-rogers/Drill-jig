package org.apache.drill.jig.direct;

import java.util.Properties;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.RemoteServiceSet;

/**
 * Factory for a Drillbit connection. Drill has a number of ways to build
 * connections; this factory class tries to isolate the application from the
 * details.
 */

public class DrillConnectionFactory
{
  public enum Mode { EMBEDDED, DRILLBIT_DIRECT, ZK_INDIRECT }

  private static final String DRILLBIT_PROPERTY = "drillbit";

  private DrillConnectionFactory.Mode mode;
  private DrillClientContext clientContext;
  Properties connectProps = new Properties( );
  private String zkConnectString;
  
  public DrillConnectionFactory( )
  {      
    this.clientContext = DrillClientContext.instance( );
  }
  
  public DrillConnectionFactory embedded( ) {
    assertNoMode( );
    mode = Mode.EMBEDDED;
    return this;
  }
  
  private void assertNoMode() {
    if ( mode != null )
      throw new DirectConnectionError( "Connection mode already set to " + mode );
  }

  public DrillConnectionFactory drillbit( String host ) {
    assertNoMode( );
    mode = Mode.DRILLBIT_DIRECT;
    return withProperty( DRILLBIT_PROPERTY, host );
  }
  
  public DrillConnectionFactory drillbit( String host, int port ) {
    assertNoMode( );
    mode = Mode.DRILLBIT_DIRECT;
    return withProperty( DRILLBIT_PROPERTY, host + ":" + port );
  }
  
  public DrillConnectionFactory viaZk( ) {
    assertNoMode( );
    mode = Mode.ZK_INDIRECT;
    return this;
  }
  
  public DrillConnectionFactory viaZk( String connectStr ) {
    assertNoMode( );
    mode = Mode.ZK_INDIRECT;
    zkConnectString = connectStr;
    return this;
  }
  
  public DrillConnectionFactory withCredentials( ) {
    return this;
  }
  
  public DrillConnectionFactory withLogin( String userName, String pwd ) {
    connectProps.put( UserSession.USER, userName );
    connectProps.put( UserSession.PASSWORD, pwd );
    return this;
  }
  
  public DrillConnectionFactory withSchema( String schema ) {
    connectProps.put( UserSession.SCHEMA, schema );
    return this;
  }
  
  public DrillConnectionFactory withProperty( String key, Object value ) {
    connectProps.put( key, value );
    return this;
  }
  
  public DrillConnectionFactory withProperties( Properties props ) {
    connectProps.putAll( props );
    return this;
  }
  
  public DirectConnection connect( ) throws DirectConnectionException {
    if ( mode == null )
      throw new DirectConnectionError( "No connection mode specified" );
    switch ( mode ) {
    case DRILLBIT_DIRECT:
      return connectDirect( );
    case EMBEDDED:
      return connectEmbedded( );
    case ZK_INDIRECT:
      return connectViaZk( );
    default:
      throw new IllegalStateException( "Unrecognized mode: " + mode );
    }
  }

  private DirectConnection connectEmbedded() throws DirectConnectionException {
    if ( ! clientContext.hasEmbeddedDrillbit( ) ) {
      throw new IllegalStateException( "Context not configured with an embedded Drillbit" );
    }
    RemoteServiceSet serviceSet = clientContext.getEmbeddedDrillbit( ).getEmbeddedServiceSet();
    return doConnect( serviceSet.getCoordinator(), null, false );
  }

  /**
   * Implementation of a direct connection. The target drillbit appears as a connection
   * property. All Drillbit connections share the context allocator. The ZK cluster
   * coordinator is null because we use the default.
   * @return
   * @throws DirectConnectionException 
   */
  
  private DirectConnection connectDirect() throws DirectConnectionException {
    return doConnect( null, null, true );
  }
  
  private DirectConnection doConnect( ClusterCoordinator coordinator, String zkConnect, boolean isDirect ) throws DirectConnectionException {
    DrillConfig config = clientContext.getConfig( );
    BufferAllocator allocator = clientContext.getRootAllocator( );
//      RemoteServiceSet serviceSet = RemoteServiceSet.getServiceSetWithFullCache(config, allocator);
//      ClusterCoordinator coordinator = null;
    try {
      DrillClient client = new DrillClient( config, coordinator, allocator, isDirect );
      
      // Oddly, the connect string is ignored for a direct connection. The connection
      // information must come in as a connection property instead.
    
      client.connect(zkConnect, connectProps);
      return new DirectConnection( client );
    } catch (RpcException e) {
      throw new DirectConnectionException( "Connect failed", e );
    }
  }

  private DirectConnection connectViaZk() throws DirectConnectionException {
    return doConnect( null, zkConnectString, false );
  }
}