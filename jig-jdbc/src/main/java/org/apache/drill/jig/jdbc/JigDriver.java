package org.apache.drill.jig.jdbc;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.client.ClientConstants;
import org.apache.drill.jig.client.ConnectionFactory;
import org.apache.drill.jig.client.DrillLoginFactory;
import org.apache.drill.jig.protocol.MessageConstants;

/**
 * Supported URL:<br>
 * <code>HEAD TAIL<code>
 * Where HEAD is one of:
 * <pre>
 * jdbc:jig:
 * jdbc:jig:drill:
 * jdbc:jig:embedded:
 * jdbc:jig:zk:
 * jdbc:jig:config:
 * </pre>
 * And<br>
 * TAIL = HOST SCHEMA TAIL<br>
 * HOST = <code>(host(:port)?)?</code>
 * SCHEMA = (/schema)?<br>
 * PROPS = <code>((?prop=value)(&prop=value)*)?</code><br>
 * <p>
 * For ZooKeeper, host is of the form:<br>
 * <code>HOST(,HOST)*</code>
 * <p>
 * Also supports the legacy formats:
 * Connection with local Drill client to a remote Drillbit via
 * ZooKeeper:
 * <pre>jdbc:drill:zk=<zk name>[:&lt;port>][,&lt;zk name2>[:&lt;port>]...
 * <directory>/&lt;cluster ID>;[schema=&lt;storage plugin>]</pre>
 * Connection with local Drill client to a remote Drillbit:
 * <pre>jdbc:drill:drillbit=&lt;node name>[:&lt;port>][,&lt;node name2>[:&lt;port>]...
 * &lt;directory>/&lt;cluster ID>[schema=&lt;storage plugin>]</pre>
 */

public class JigDriver implements Driver
{
  public static class UrlParser
  {
    public static final String conStrPattern =
        "jdbc:jig:" +           // Required prefix
        "(?:([^:]*):)?" +       // Optional method (drill, zk, etc.)
        "(?://" +
          "(?:" +
            "([^:@]+)" +        // user
            "(?::([^@]+))?" +   // password
          "@)?" +
          "([^:/?]*)" +         // host
          "(?::(\\d+))?" +      // port
        ")?" +
        "(?:/([^?]*))?" +       // Schema
        "(?:\\?(.*))?";         // Property

    // TODO: Add workspace a part of schema
    
    boolean isValid;
    String method;
    String user;
    String password;
    String host;
    int port;
    String schema;
    Map<String,String> urlProps = new HashMap<>( );
    Properties props;
    
    public UrlParser( String url ) {
      if ( ! isJigUrl( url ) )
        return;
      
      Pattern p = Pattern.compile( conStrPattern );
      Matcher m = p.matcher( url );
      if ( ! m.matches() )
        return;
      method = decode( m.group( 1 ) );
      user = decode( m.group( 2 ) );
      password = decode( m.group( 3 ) );
      host = decode( m.group( 4 ) );
      port = 0;
      if ( m.group( 5 ) != null ) {
        port = Integer.parseInt( m.group( 5 ) );
      }
      schema = decode( m.group( 6 ) );
      String propStr = m.group( 7 );
      if ( ! isBlank( propStr ) ) {
        for ( String pair : propStr.split( "&" ) ) {
          String parts[] = pair.split( "=" );
          if ( parts.length != 2 ) {
            throw new IllegalArgumentException( "Invalid URL property: " + pair );
          }
          urlProps.put( decode( parts[0] ), decode( parts[1] ) );
        }
      }
      isValid = true;
    }
    
    public static String decode( String value ) {
      if ( value == null )
        return value;
      try {
        return URLDecoder.decode( value, "UTF-8" );
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException( e );
      }
    }
    
    public static boolean isJigUrl( String url ) {
      return url.startsWith( JDBCConstants.JIG_PREFIX );
    }

    public void setProperties(Properties info) {
      props = info;
    }
    
    public String lookup( String key ) {
      String value = urlProps.get( key );
      if ( value != null )
        return value;
      if ( props == null )
        return null;
      return props.getProperty( key );
    }
    
    public String getMethod( ) {
      if ( ! isBlank( method ) ) {
        return method;
      }
      String value = lookup( JDBCConstants.METHOD_PROPERTY );
      if ( ! isBlank( value ) ) {
        return value;
      }
      return JDBCConstants.DRILL_PRESS_METHOD;
    }
    
    public String getHost( ) {
      if ( ! isBlank( host ) ) {
        return host;
      }
      String value = lookup( JDBCConstants.HOST_PROPERTY );
      if ( ! isBlank( value ) ) {
        return value;
      }
      return MessageConstants.DEFAULT_HOST;
    }
    
    public int getPort( ) {
      if ( port != 0 )
        return port;
      String value = lookup( JDBCConstants.PORT_PROPERTY );
      if ( ! isBlank( value ) )
        return Integer.parseInt( value );
      return MessageConstants.DEFAULT_PORT;
    }
    
    public static boolean isBlank( String value ) {
      return value == null  ||  value.isEmpty();
    }

    public String getUser() {
      if ( ! isBlank( user ) )
        return user;
      return lookup( JDBCConstants.USER_PROPERTY );
    }
    
    public String getPassword( ) {
      if ( ! isBlank( password ) )
        return password;
      return lookup( JDBCConstants.PASSWORD_PROPERTY );
    }

    public String getSchema() {
      if ( ! isBlank( schema ) )
        return schema;
      return lookup( JDBCConstants.SCHEMA_PROPERTY );
    }
  }

  static {
    try {
      DriverManager.registerDriver(new JigDriver() );
    } catch (SQLException e) {
      throw new IllegalStateException( "Jig driver failed to register", e );
    }
  }
  
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if ( ! UrlParser.isJigUrl( url ) ) {
      return null;
    }
    UrlParser parser = new UrlParser( url );
    if ( ! parser.isValid ) {
      return null;
    }
    
    parser.setProperties( info );
    
    if ( JDBCConstants.DRILL_PRESS_METHOD.equals( parser.getMethod() ) ) {
      return buildDrillPressConnection( parser );
    }
    throw new SQLException( "jdbc:jig: method not supported: " + parser.method );
  }

  private Connection buildDrillPressConnection(UrlParser parser) throws JigWrapperException {
    ConnectionFactory factory = new ConnectionFactory( );
    String host = parser.getHost();
    if ( ! UrlParser.isBlank( host ) )
      factory.toHost( host );
    int port = parser.getPort();
    if ( port != 0 )
      factory.onPort( port );
    
    try {
      DrillLoginFactory loginFactory = factory.connect();
      String user = parser.getUser( );
      if ( UrlParser.isBlank( user ) ) {
        return new JigJdbcConnection( loginFactory.login( ), parser.getSchema( ) );
      }
      else {
        return new JigJdbcConnection( loginFactory.login( user, parser.getPassword( ) ), parser.getSchema( ) );
      }
    } catch (JigException e) {
      throw new JigWrapperException( e );
    }   
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return new UrlParser( url ).isValid;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    UrlParser parser = new UrlParser( url );
    if ( info != null )
      parser.setProperties( info );
    
    List<DriverPropertyInfo> props = new ArrayList<>( );
    
    DriverPropertyInfo prop = new DriverPropertyInfo( JDBCConstants.METHOD_PROPERTY, parser.getMethod( ) );
    prop.description = "Connection method";
    prop.choices = new String[] {
        JDBCConstants.DRILL_DIRECT_METHOD,
        JDBCConstants.DRILL_PRESS_METHOD,
        JDBCConstants.EMBEDDED_METHOD,
        JDBCConstants.ZK_DIRECT_METHOD
    };
    props.add( prop );
    
    prop = new DriverPropertyInfo( JDBCConstants.USER_PROPERTY, parser.getUser() );
    prop.description = "User name";
    props.add( prop );
    
    prop = new DriverPropertyInfo( JDBCConstants.PASSWORD_PROPERTY, parser.getPassword() );
    prop.description = "User password";
    props.add( prop );
    
    prop = new DriverPropertyInfo( JDBCConstants.HOST_PROPERTY, parser.getHost() );
    prop.description = "Jig, Drill or ZooKeeper host";
    props.add( prop );
    
    int port = parser.getPort();
    prop = new DriverPropertyInfo( JDBCConstants.PORT_PROPERTY,
        port == 0 ? null : Integer.toString( port ) );
    prop.description = "Jig, Drill or ZooKeeper port";
    props.add( prop );
    
    prop = new DriverPropertyInfo( JDBCConstants.SCHEMA_PROPERTY, parser.getSchema() );
    prop.description = "Drill default schema (storage plugin)";
    props.add( prop );
    
    DriverPropertyInfo propInfo[] = new DriverPropertyInfo[ props.size() ];
    return props.toArray( propInfo );
  }

  @Override
  public int getMajorVersion() {
    return ClientConstants.MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return ClientConstants.MINOR_VERSION;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO Auto-generated method stub
    return null;
  }

}
