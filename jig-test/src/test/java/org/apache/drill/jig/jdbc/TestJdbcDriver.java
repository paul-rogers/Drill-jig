package org.apache.drill.jig.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.drill.jig.client.ClientConstants;
import org.apache.drill.jig.protocol.MessageConstants;
import org.apache.drill.jig.server.DrillPressContext;
import org.apache.drill.jig.server.EmbeddedDrillPress;
import org.apache.drill.jig.jdbc.JigDriver.UrlParser;
import org.junit.Test;

public class TestJdbcDriver
{

  @Test
  public void testUrlParserJig( ) {
    {
      UrlParser p = new UrlParser( "jdbc:jig:" );
      assertTrue( p.isValid );
      assertNull( p.method );
      assertEquals( null, p.host );
      assertEquals( 0, p.port );
      assertNull( p.schema );
      assertTrue( p.urlProps.isEmpty() );
      assertEquals( JDBCConstants.DRILL_PRESS_METHOD, p.getMethod( ) );
      assertEquals( MessageConstants.DEFAULT_HOST, p.getHost() );
      assertEquals( MessageConstants.DEFAULT_PORT, p.getPort() );
      assertNull( p.getSchema() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig://host" );
      assertTrue( p.isValid );
      assertNull( p.method );
      assertEquals( "host", p.host );
      assertEquals( 0, p.port );
      assertNull( p.schema );
      assertTrue( p.urlProps.isEmpty() );
      assertEquals( "host", p.getHost() );
      assertEquals( MessageConstants.DEFAULT_PORT, p.getPort() );
      assertNull( p.getSchema() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig://ahost:123" );
      assertTrue( p.isValid );
      assertNull( p.method );
      assertEquals( "ahost", p.host );
      assertEquals( 123, p.port );
      assertNull( p.schema );
      assertTrue( p.urlProps.isEmpty() );
      assertEquals( "ahost", p.getHost() );
      assertEquals( 123, p.getPort() );
      assertNull( p.getSchema() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig://ahost:123/myschema" );
      assertTrue( p.isValid );
      assertNull( p.method );
      assertEquals( "ahost", p.host );
      assertEquals( 123, p.port );
      assertEquals( "myschema", p.schema );
      assertTrue( p.urlProps.isEmpty() );
      assertEquals( "myschema", p.getSchema() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig://ahost:123/myschema?prop1=value1" );
      assertTrue( p.isValid );
      assertNull( p.method );
      assertEquals( "ahost", p.host );
      assertEquals( 123, p.port );
      assertEquals( "myschema", p.schema );
      assertEquals( 1, p.urlProps.size( ) );
      assertEquals( "value1", p.urlProps.get( "prop1" ) );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig:?prop1=value1" );
      assertTrue( p.isValid );
      assertNull( p.method );
      assertEquals( null, p.host );
      assertEquals( 0, p.port );
      assertEquals( null, p.schema );
      assertEquals( 1, p.urlProps.size( ) );
      assertEquals( "value1", p.urlProps.get( "prop1" ) );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig://ahost:123/myschema?prop1=value1&prop2=value2" );
      assertTrue( p.isValid );
      assertNull( p.method );
      assertEquals( "ahost", p.host );
      assertEquals( 123, p.port );
      assertEquals( "myschema", p.schema );
      assertEquals( 2, p.urlProps.size( ) );
      assertEquals( "value1", p.urlProps.get( "prop1" ) );
      assertEquals( "value2", p.urlProps.get( "prop2" ) );
    }
    
    // Property style: host, port and schema as properties
    
    {
      UrlParser p = new UrlParser( "jdbc:jig:?host=myhost&port=123&schema=mySchema" );
      assertTrue( p.isValid );
      assertNull( p.method );
      assertEquals( null, p.host );
      assertEquals( 0, p.port );
      assertEquals( null, p.schema );
      assertEquals( 3, p.urlProps.size( ) );
      assertEquals( "myhost", p.urlProps.get( "host" ) );
      assertEquals( "123", p.urlProps.get( "port" ) );
      assertEquals( "mySchema", p.urlProps.get( "schema" ) );
      assertEquals( "myhost", p.getHost() );
      assertEquals( 123, p.getPort() );
      assertEquals( "mySchema", p.getSchema() );
    }
    
    // User and password
    
    {
      UrlParser p = new UrlParser( "jdbc:jig://fred@host" );
      assertTrue( p.isValid );
      assertEquals( "host", p.host );
      assertEquals( "fred", p.user );
      assertNull( p.password );
      assertNull( p.getSchema() );
      assertEquals( "fred", p.getUser() );
      assertNull( p.getPassword() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig://fred:p%40st@host" );
      assertTrue( p.isValid );
      assertEquals( "host", p.host );
      assertEquals( "fred", p.user );
      assertEquals( "p@st", p.password );
      assertNull( p.getSchema() );
      assertEquals( "fred", p.getUser() );
      assertEquals( "p@st", p.getPassword() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig://host?user=fred&password=p%40st" );
      assertTrue( p.isValid );
      assertEquals( "host", p.host );
      assertNull( p.user );
      assertNull( p.password );
      assertNull( p.getSchema() );
      assertEquals( "fred", p.getUser() );
      assertEquals( "p@st", p.getPassword() );
    }
    
    // Properties
    
    Properties props = new Properties( );
    props.put( JDBCConstants.USER_PROPERTY, "fred" );
    props.put( JDBCConstants.PASSWORD_PROPERTY, "p@st" );
    props.put( JDBCConstants.HOST_PROPERTY, "ahost" );
    props.put( JDBCConstants.PORT_PROPERTY, "123" );
    props.put( JDBCConstants.SCHEMA_PROPERTY, "mySchema" );
    {
      UrlParser p = new UrlParser( "jdbc:jig:" );
      p.setProperties( props );
      assertEquals( "fred", p.getUser() );
      assertEquals( "p@st", p.getPassword() );
      assertEquals( "ahost", p.getHost() );
      assertEquals( 123, p.getPort() );
      assertEquals( "mySchema", p.getSchema() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig://wilma:barney@bhost:456/aschema" );
      assertTrue( p.isValid );
      p.setProperties( props );
      assertEquals( "wilma", p.getUser() );
      assertEquals( "barney", p.getPassword() );
      assertEquals( "bhost", p.getHost() );
      assertEquals( 456, p.getPort() );
      assertEquals( "aschema", p.getSchema() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig:?user=wilma&password=barney&host=bhost&port=456&schema=aschema" );
      assertTrue( p.isValid );
      p.setProperties( props );
      assertEquals( "wilma", p.getUser() );
      assertEquals( "barney", p.getPassword() );
      assertEquals( "bhost", p.getHost() );
      assertEquals( 456, p.getPort() );
      assertEquals( "aschema", p.getSchema() );
    }
    {
      UrlParser p = new UrlParser( "jdbc:jig:?host=bhost&schema=aschema" );
      assertTrue( p.isValid );
      p.setProperties( props );
      assertEquals( "fred", p.getUser() );
      assertEquals( "p@st", p.getPassword() );
      assertEquals( "bhost", p.getHost() );
      assertEquals( "aschema", p.getSchema() );
    }
  }

  @Test
  public void testDriverProps( ) throws SQLException {
    JigDriver driver = new JigDriver( );
    DriverPropertyInfo[] props = driver.getPropertyInfo( 
        "jdbc:jig:?user=wilma&password=barney&host=bhost&port=456&schema=aschema", null );
    assertEquals( JDBCConstants.METHOD_PROPERTY, props[0].name );
    assertEquals( JDBCConstants.DRILL_PRESS_METHOD, props[0].value );
    
    assertEquals( JDBCConstants.USER_PROPERTY, props[1].name );
    assertEquals( "wilma", props[1].value );
    
    assertEquals( JDBCConstants.PASSWORD_PROPERTY, props[2].name );
    assertEquals( "barney", props[2].value );
    
    assertEquals( JDBCConstants.HOST_PROPERTY, props[3].name );
    assertEquals( "bhost", props[3].value );
    
    assertEquals( JDBCConstants.PORT_PROPERTY, props[4].name );
    assertEquals( "456", props[4].value );
    
    assertEquals( JDBCConstants.SCHEMA_PROPERTY, props[5].name );
    assertEquals( "aschema", props[5].value );
  }

  @Test
  public void testDriverAttribs( ) throws SQLException {
    JigDriver driver = new JigDriver( );
    assertFalse( driver.jdbcCompliant() );
    assertEquals( ClientConstants.MAJOR_VERSION, driver.getMajorVersion() );
    assertEquals( ClientConstants.MINOR_VERSION, driver.getMinorVersion() );
    assertTrue( driver.acceptsURL( "jdbc:jig:" ) );
    assertFalse( driver.acceptsURL( "jdbc:mysql:" ) );
  }

  @Test
  public void testDriverConnectJig( ) throws SQLException {
    DrillPressContext context = new DrillPressContext( )
        .direct( "localhost" );
    EmbeddedDrillPress drillPress = new EmbeddedDrillPress( context )
        .start( );
    
    JigDriver driver = new JigDriver( );
    Connection conn = driver.connect( "jdbc:jig:?user=progers&password=Dar%400616", null );
    conn.close();
    drillPress.stop();
  }
}
