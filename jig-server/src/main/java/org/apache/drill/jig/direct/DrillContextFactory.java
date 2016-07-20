package org.apache.drill.jig.direct;

import java.io.File;
import java.util.Properties;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;

/**
 * Builds the one and only Drill Context per process. The context
 * holds the single Drill configuration and, if requested, the single
 * embedded Drillbit.
 */

public class DrillContextFactory
{
  private boolean isEmbedded;
  private DrillConfig config;
  private File configFile;
  private Properties contextProps = new Properties( );
  
  public DrillContextFactory withProperty( String key, Object value ) {
    contextProps.put( key, value );
    return this;
  }
  
  public DrillContextFactory withProperties( Properties props ) {
    contextProps.putAll( props );
    return this;
  }
  
  public DrillContextFactory withConfigFile( File file ) {
    this.configFile = file;
    return this;
  }
  
  public DrillContextFactory asClient( ) {
    isEmbedded = false;
    return this;
  }
  
  public DrillContextFactory withEmbeddedDrillbit( )
  {
    contextProps.put(ExecConstants.HTTP_ENABLE, "false");
    isEmbedded = true;
    return this;
  }
  
  public DrillContextFactory withWebServer( ) {
    contextProps.put(ExecConstants.HTTP_ENABLE, "true");
    return this;
  }
  
  public DrillContextFactory readOnly( ) {
    contextProps.put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
    return this;
  }
  
  public DrillClientContext build( ) {
    if ( DrillClientContext.isCreated( ) ) {
      throw new DrillSessionError( "Session already created" );
    }
    String configPath = null;
    
    // Work around the odd collection of constructors provided.
    
    if ( configFile != null ) {
      configPath = configFile.getAbsolutePath();
    }
    if ( ! isEmbedded  &&  ! contextProps.isEmpty() ) {
      throw new DrillSessionError( "Cannot specify custom properties for client session" );
    }
    if ( ! isEmbedded )
      config = DrillConfig.create( configPath, false );
    else if ( configPath != null && ! contextProps.isEmpty() ) {
      throw new DrillSessionError( "Cannot specify both custom properties and a config file" );
    }
    else if ( configPath != null )
      config = DrillConfig.create( configPath, isEmbedded );
    else
      config = DrillConfig.create( contextProps );
    
    return DrillClientContext.init( config );
  }
}