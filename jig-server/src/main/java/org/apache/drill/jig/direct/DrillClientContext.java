package org.apache.drill.jig.direct;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;

public class DrillClientContext implements AutoCloseable
{
  private static DrillClientContext instance;
  private final DrillConfig config;
  private BufferAllocator allocator;
  private Drillbit embeddedDrillbit;
  private RemoteServiceSet embeddedServiceSet;
  
  public DrillClientContext( )
  {
    this( DrillConfig.create() );
  }
  
  public boolean hasEmbeddedDrillbit() {
    return embeddedDrillbit != null;
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
      throw new DrillSessionError( "Already initialized" );
    instance = new DrillClientContext( );
  }
  
  public static DrillClientContext init( DrillConfig config )
  {
    if ( instance != null )
      throw new DrillSessionError( "Already initialized" );
    instance = new DrillClientContext( config );
    return instance;
  }
  
  public void startEmbedded( ) throws DrillSessionException {
    if ( embeddedDrillbit != null )
      throw new DrillSessionError( "Embedded Drillbit already started" );
    if ( embeddedServiceSet == null )
      embeddedServiceSet = RemoteServiceSet.getLocalServiceSet();
    try {
      embeddedDrillbit = new Drillbit(config, embeddedServiceSet);
      embeddedDrillbit.run();
    } catch (Exception e) {
      throw new DrillSessionException( "Failed to start embedded Drillbit", e );
    }
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
    stopEmbeddedDrillbit( );
  }
  
  public void stopEmbeddedDrillbit( )
  {
    if ( embeddedDrillbit != null ) {
      embeddedDrillbit.close();
      embeddedDrillbit = null;
    }
  }
  
  public RemoteServiceSet getEmbeddedServiceSet( ) {
    return embeddedServiceSet;
  }
}