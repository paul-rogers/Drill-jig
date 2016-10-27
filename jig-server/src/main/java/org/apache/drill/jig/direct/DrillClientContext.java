package org.apache.drill.jig.direct;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;

/**
 * Context for a Drill connection that supports the various Drill connection
 * modes: embedded, direct, ZooKeeper and via the config file.
 */

public class DrillClientContext implements AutoCloseable
{
  private static DrillClientContext instance;
  private final DrillConfig config;
  private BufferAllocator allocator;
  private EmbeddedDrillbit embeddedDrillbit;
  
  public DrillClientContext( )
  {
    this( DrillConfig.create() );
  }
  
  public boolean hasEmbeddedDrillbit() {
    return embeddedDrillbit != null;
  }
  
  public EmbeddedDrillbit getEmbeddedDrillbit( ) {
    return embeddedDrillbit;
  }

  private DrillClientContext( DrillConfig config )
  {
    this.config = config;
  }
  
  BufferAllocator getRootAllocator() {
    if ( allocator == null )
      allocator = RootAllocatorFactory.newRoot(config);
    return allocator;
  }

  public DrillConfig getConfig( ) {
    return config;
  }
  
  public static void init( )
  {
    if ( instance != null )
      throw new DirectConnectionError( "Already initialized" );
    instance = new DrillClientContext( );
  }
  
  public static DrillClientContext init( DrillConfig config )
  {
    if ( instance != null )
      throw new DirectConnectionError( "Already initialized" );
    instance = new DrillClientContext( config );
    return instance;
  }
  
  public static DrillClientContext instance( ) {
    if ( instance == null )
      init( );
    return instance;
  }
  
  public static boolean isCreated( ) {
    return instance != null;
  }

  @Override
  public void close() throws Exception {
    if ( embeddedDrillbit != null )
      embeddedDrillbit.close( );
  }

  protected void startEmbeddedDrillbit() throws DirectConnectionException {
    embeddedDrillbit = new EmbeddedDrillbit( this );
  }
  
}