package org.apache.drill.jig.direct;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;

public class EmbeddedDrillbit {
  private Drillbit drillbit;
  private RemoteServiceSet embeddedServiceSet;

  /**
   * Start an embedded Drillbit.
   * 
   * @throws DirectConnectionException
   */
  
  protected EmbeddedDrillbit( DrillClientContext context ) throws DirectConnectionException {
    if ( drillbit != null )
      throw new DirectConnectionError( "Embedded Drillbit already started" );
    if ( embeddedServiceSet == null )
      embeddedServiceSet = RemoteServiceSet.getLocalServiceSet();
    try {
      drillbit = new Drillbit(context.getConfig(), embeddedServiceSet);
      drillbit.run();
    } catch (Exception e) {
      throw new DirectConnectionException( "Failed to start embedded Drillbit", e );
    }
  }
  
  /**
   * Stop the embedded Drillbit.
   */
  
  protected void close( )
  {
    if ( drillbit != null ) {
      drillbit.close();
      drillbit = null;
    }
  }
  
  public RemoteServiceSet getEmbeddedServiceSet( ) {
    return embeddedServiceSet;
  }
  
  public void defineWorkspace( String pluginName, String schemaName, String path, String defaultFormat ) throws ExecutionSetupException {
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin(pluginName);
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();
    final WorkspaceConfig newTmpWSConfig = new WorkspaceConfig(path, true, defaultFormat);

    pluginConfig.workspaces.remove(schemaName);
    pluginConfig.workspaces.put(schemaName, newTmpWSConfig);

    pluginRegistry.createOrUpdate(pluginName, pluginConfig, true);
  }
}
