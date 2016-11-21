package org.apache.drill.jig.server;

import java.net.URL;

import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.jig.protocol.MessageConstants;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Drillpress configuration. Configuration comes from a number of sources.
 * In order of precedence:
 * <ol>
 * <li>Runtime settings expressed as JVM system properties: -Dprop=value options.</li>
 * <li>User-defined settings: drillpress.conf.</li>
 * <li>Distribution-specific settings (defined by any software
 * distributions that choose to include Jig.):
 * drillpress-distrib.conf</li>
 * <li>Drillpress defaults (defined by the Jig project.)</li>
 * <li>Drill's configuration (including drill-overrides.conf.)</li>
 * </ol>
 * 
 * Items higher in the list override those higher in the list. Said another
 * way the bottom items are defaults for properties not set in higher items.
 * <p>
 * Configuration files use the same HOCON format as Drill.
 * <p>
 * Drill properties are included for convenience. This allows Drillpress code
 * to always use this configuration object independent of whether the property
 * in question is defined in Drill or Jig.
 * <p>
 * Borrows heavily from the Drill-on-YARN configuration implementation.
 * Uses the same file hierarchy and the same integration with Drill's
 * configuration.
 */

public class DrillPressConfig {

  public static final String DEFAULTS_FILE_NAME = "/drillpress-defaults.conf";
  public static final String DISTRIB_FILE_NAME = "drillpress-distrib.conf";
  public static final String CONFIG_FILE_NAME = "drillpress.conf";
  
  public static final String DRILLPRESS_PARENT = "drillpress";
  public static final String DRILL_PARENT = append( DRILLPRESS_PARENT, "drill" );
  
  public static final String DRILLPRESS_PORT = append( DRILLPRESS_PARENT, "port" );
  public static final String DRILL_AUTH = append( DRILL_PARENT, "auth" );
  
  public static final String OPEN_SECURITY = MessageConstants.OPEN_LOGIN;
  public static final String BASIC_SECURITY = MessageConstants.USER_PWD_LOGIN;
  
  public static final String DRILL_CONNECT_METHOD = append( DRILL_PARENT, "method" );
  public static final String DRILL_HOST = append( DRILL_PARENT, "host" );
  public static final String DRILL_USER_PORT = append( DRILL_PARENT, "user-port" );
  public static final String ZK_CONNECT = append( DRILL_PARENT, "zk-connect" );

  private static DrillPressConfig instance;
  private static DrillConfig drillConfig;
  private Config config;
  
  public static String append( String parent, String key ) {
    return parent + "." + key;
  }

  public static DrillPressConfig init( ) {
    instance = new DrillPressConfig( );
    instance.doLoad( Thread.currentThread().getContextClassLoader() );
    return instance;
  }
  
  protected void doLoad( ClassLoader cl )
  {
    Config drillConfig = loadDrillConfig( );

    // Resolution order, larger numbers take precedence.
    // 1. Drill-on-YARN defaults.
    // File is at root of the package tree.

    URL url = DrillPressConfig.class.getResource( DEFAULTS_FILE_NAME );
    if ( url == null ) {
      throw new IllegalStateException( "Drillpress defaults file is required: " + DEFAULTS_FILE_NAME );
    }
    config = ConfigFactory.parseURL(url).withFallback( drillConfig );

    // 2. Optional distribution-specific configuration-file.
    // (Lets a vendor, for example, specify the default DFS upload location without
    // tinkering with the user's own settings.

    url = cl.getResource( DISTRIB_FILE_NAME );
    if ( url != null ) {
      config = ConfigFactory.parseURL(url).withFallback( config );
    }

    // 3. User's Drill-on-YARN configuration.

    url = cl.getResource( CONFIG_FILE_NAME );
    if ( url != null ) {
      config = ConfigFactory.parseURL(url).withFallback( config );
    }

    // 4. System properties
    // Allows -Dfoo=bar on the command line.
    // But, note that substitutions are NOT allowed in system properties!

    config = ConfigFactory.systemProperties( ).withFallback( config );

    // Resolution allows ${foo.bar} syntax in values, but only for values
    // from config files, not from system properties.

    config = config.resolve();
  }

  private static Config loadDrillConfig() {
    drillConfig = DrillConfig.create( CommonConstants.CONFIG_OVERRIDE_RESOURCE_PATHNAME );
    return drillConfig.resolve();
  }

  public DrillConfig getDrillConfig( ) {
    return drillConfig;
  }

  
  public static DrillPressConfig instance( ) {
    assert instance != null;
    return instance;
  }
  
  public static Config config( ) {
    return instance( ).getConfig( );
  }

  private Config getConfig() {
    return config;
  }
}
