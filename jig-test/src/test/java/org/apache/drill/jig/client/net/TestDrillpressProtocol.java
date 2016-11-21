package org.apache.drill.jig.client.net;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.api.AlterSessionKeys;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.proto.ColumnSchema;
import org.apache.drill.jig.proto.ExecuteRequest;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesResponse;
import org.apache.drill.jig.proto.LoginRequest;
import org.apache.drill.jig.proto.PropertyDef;
import org.apache.drill.jig.proto.PropertyValue;
import org.apache.drill.jig.proto.QueryRequest;
import org.apache.drill.jig.proto.SchemaResponse;
import org.apache.drill.jig.proto.SuccessResponse;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.protocol.HelloRequest;
import org.apache.drill.jig.protocol.HelloResponse;
import org.apache.drill.jig.protocol.MessageConstants;
import org.apache.drill.jig.server.DrillPressContext;
import org.apache.drill.jig.server.EmbeddedDrillPress;
import org.junit.Test;

/**
 * Test the Jig wire protocol using an in-process Drillpress.
 * Requires a running Drillbit on localhost.
 */

public class TestDrillpressProtocol
{

  @Test
  public void test() throws JigException {
    DrillPressContext context = new DrillPressContext( )
        .direct( "localhost" );
    EmbeddedDrillPress drillPress = new EmbeddedDrillPress( context )
        .start( );
    
    JigClientFacade client = new JigClientFacade( );
    client.connect( "localhost", MessageConstants.DEFAULT_PORT );
    
    // Hello
    
    {
      HelloRequest req = new HelloRequest( DrillPressContext.SERVER_VERSION,
          DrillPressContext.LOWEST_SUPPORTED_VERSION );
      HelloResponse resp = client.hello( req );
      assertEquals( DrillPressContext.SERVER_VERSION, resp.serverVersion );
      assertEquals( DrillPressContext.SERVER_VERSION, resp.sessionVersion );
    }
    
    // List Logins
    
    {
      ListLoginsResponse resp = client.getLoginMethods( );
      List<String> methods = resp.getLoginTypesList();
      assertEquals( 1, methods.size( ) );
      assertTrue( methods.contains( MessageConstants.OPEN_LOGIN ) );
//      assertTrue( methods.contains( MessageConstants.USER_PWD_LOGIN ) );
    }
    
    // Get Login Properties
    
    {
      LoginPropertiesResponse resp = client.getLoginProperties( MessageConstants.OPEN_LOGIN );
      assertEquals( MessageConstants.OPEN_LOGIN, resp.getLoginType() );
      List<PropertyDef> props = resp.getPropertiesList();
      
      // An empty property list becomes a null list after ser/de.
      
      assertNull( props );
    }
    
    // Login
    
    {
      LoginRequest req = new LoginRequest( )
          .setLoginType( MessageConstants.OPEN_LOGIN );
      List<PropertyValue> props = new ArrayList<>( );
      req.setPropertiesList( props );
      client.login( req );     
    }
    
    // Execute statement
    
    {
      ExecuteRequest req = new ExecuteRequest( )
          .setStatement( "ALTER SESSION SET `" +
              AlterSessionKeys.MAX_WIDTH_PER_NODE + "` = 2" );
      SuccessResponse resp = client.executeStmt( req );
      assertEquals( 0, resp.getRowCount( ) );
    }
    
    // Execute query and fetch unserialized results
    
    {
      String stmt = "SELECT * FROM cp.`employee.json` LIMIT 20";
      QueryRequest req = new QueryRequest( )
          .setStatement( stmt )
          .setMaxResponseSizeK( 4096 )
          .setMaxWaitSec( 75 );
      client.executeQuery( req );
    }
    
    // First fetch: schema
    
    {
      DataResponse resp = client.getResults();
      assertEquals( DataResponse.Type.SCHEMA, resp.type );
      assertNotNull( resp.schema );
      SchemaResponse schema = resp.schema;
      List<ColumnSchema> cols = schema.getColumnsList();
      assertEquals( 16, cols.size() );
    }
    
    // Second fetch: data
    
    {
      DataResponse resp = client.getResults();
      assertEquals( DataResponse.Type.DATA, resp.type );
      assertNotNull( resp.data );
    }
    
    // Third fetch: eof
    
    {
      DataResponse resp = client.getResults();
      assertEquals( DataResponse.Type.EOF, resp.type );
    }
    
    // Goodbye & close
    
    {
      client.close( );
    }
    
    drillPress.stop();
  }

}
