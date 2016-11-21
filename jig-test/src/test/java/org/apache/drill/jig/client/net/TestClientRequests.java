package org.apache.drill.jig.client.net;

import static org.junit.Assert.*;

import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.proto.ColumnSchema;
import org.apache.drill.jig.proto.ExecuteRequest;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesResponse;
import org.apache.drill.jig.proto.LoginRequest;
import org.apache.drill.jig.proto.PropertyDef;
import org.apache.drill.jig.proto.PropertyType;
import org.apache.drill.jig.proto.PropertyValue;
import org.apache.drill.jig.proto.QueryRequest;
import org.apache.drill.jig.proto.SchemaResponse;
import org.apache.drill.jig.proto.SuccessResponse;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.protocol.HelloRequest;
import org.apache.drill.jig.protocol.HelloResponse;
import org.apache.drill.jig.protocol.MessageConstants;
import org.apache.drill.jig.test.CompareFiles;

/**
 * Low-level tests of the client requests by using a mock server
 * that returns canned, expected responses.
 */

public class TestClientRequests
{

  @Test
  public void test() throws JigException, InterruptedException {
    StringWriter out = new StringWriter( );
    MockServerThread serverThread = new MockServerThread( new PrintWriter( out ) );
    serverThread.start();
    serverThread.readyLatch.await();

    JigClientFacade client = new JigClientFacade( );
    client.connect( "localhost", MessageConstants.DEFAULT_PORT );

    // Hello

    {
      HelloRequest req = new HelloRequest( 2, 1 );
      HelloResponse resp = client.hello( req );
      assertEquals( 3, resp.serverVersion );
      assertEquals( 2, resp.sessionVersion );
    }

    // List Logins

    {
      ListLoginsResponse resp = client.getLoginMethods( );
      List<String> methods = resp.getLoginTypesList();
      assertEquals( 2, methods.size( ) );
      assertTrue( methods.contains( MessageConstants.OPEN_LOGIN ) );
      assertTrue( methods.contains( MessageConstants.USER_PWD_LOGIN ) );
    }

    // Get Login Properties

    {
      LoginPropertiesResponse resp = client.getLoginProperties( MessageConstants.USER_PWD_LOGIN );
      assertEquals( MessageConstants.USER_PWD_LOGIN, resp.getLoginType() );
      List<PropertyDef> props = resp.getPropertiesList();
      assertEquals( 2, props.size() );
      PropertyDef prop = props.get( 0 );
      assertEquals( "user-name", prop.getName() );
      assertEquals( PropertyType.STRING, prop.getType() );
      prop = props.get( 1 );
      assertEquals( "password", prop.getName() );
      assertEquals( PropertyType.STRING, prop.getType() );
    }

    // Login

    {
      LoginRequest req = new LoginRequest( )
          .setLoginType( MessageConstants.USER_PWD_LOGIN );
      List<PropertyValue> props = new ArrayList<>( );
      PropertyValue prop = new PropertyValue( )
          .setName( "user-name" )
          .setValue( "bob" );
      props.add( prop );
      prop = new PropertyValue( )
          .setName( "password" )
          .setValue( "secret" );
      props.add( prop );
      req.setPropertiesList( props );
      client.login( req );
    }

    // Execute statement

    {
      ExecuteRequest req = new ExecuteRequest( )
          .setStatement( "ALTER SESSION SET foo = \"bar\";" );
      SuccessResponse resp = client.executeStmt( req );
      assertEquals( 10, resp.getRowCount( ) );
    }

    // Execute query and fetch simulated results

    {
      QueryRequest req = new QueryRequest( )
          .setStatement( "SELECT * FROM foo;" )
          .setMaxResponseSizeK( 2048 )
          .setMaxWaitSec( 75 );
      client.executeQuery( req );
    }

    // First fetch, no data

    {
      DataResponse resp = client.getResults();
      assertEquals( DataResponse.Type.NO_DATA, resp.type );
      assertNotNull( resp.info );
      assertEquals( "00100", resp.info.getSqlCode() );
      assertEquals( 20, resp.info.getCode().intValue() );
      assertEquals( "Working", resp.info.getMessage() );
    }

    // Second fetch: schema

    {
      DataResponse resp = client.getResults();
      assertEquals( DataResponse.Type.SCHEMA, resp.type );
      assertNotNull( resp.schema );
      SchemaResponse schema = resp.schema;
      List<ColumnSchema> cols = schema.getColumnsList();
      assertEquals( 2, cols.size() );
      ColumnSchema col = cols.get( 0 );
      assertEquals( "first", col.getName() );
      assertEquals( DataType.STRING.typeCode(), col.getType() );
      assertEquals( 1, col.getNullable() );
      col = cols.get( 1 );
      assertEquals( "second", col.getName() );
      assertEquals( DataType.STRING.typeCode(), col.getType() );
      assertEquals( 0, col.getNullable() );
    }

    // Third fetch: simulated data

    {
      DataResponse resp = client.getResults();
      assertEquals( DataResponse.Type.DATA, resp.type );
      assertNotNull( resp.data );
      String value = new String( resp.data );
      assertEquals( "ABC|xyz", value );
    }

    // Fourth fetch: eof

    {
      DataResponse resp = client.getResults();
      assertEquals( DataResponse.Type.EOF, resp.type );
    }

    // Goodbye & close

    {
      client.close( );
    }

    serverThread.join();

    assertTrue( CompareFiles.compareResource( "/protocol-test.txt", out.toString() ) );
  }

}
