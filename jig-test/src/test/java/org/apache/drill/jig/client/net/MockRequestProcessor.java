package org.apache.drill.jig.client.net;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.drillpress.net.RequestProcessor;
import org.apache.drill.jig.proto.ColumnSchema;
import org.apache.drill.jig.proto.ExecuteRequest;
import org.apache.drill.jig.proto.InformationResponse;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesRequest;
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

public class MockRequestProcessor implements RequestProcessor
{
  private int step;
  private PrintWriter out;

  public MockRequestProcessor(PrintWriter out) {
    this.out = out;
    if ( out == null )
      out = new PrintWriter( new OutputStreamWriter( System.out ) );
  }

  @Override
  public HelloResponse hello(HelloRequest request) {
    out.println( "REQ: Hello(" + request.clientVersion + ", " + request.lowestClientVersion + ")" );
    return new HelloResponse( request.clientVersion + 1, request.clientVersion );
  }

  @Override
  public ListLoginsResponse listLogins() {
    out.println( "REQ: List Logins" );
    List<String> logins = new ArrayList<>( );
    logins.add( MessageConstants.OPEN_LOGIN );
    logins.add( MessageConstants.USER_PWD_LOGIN );
    return new ListLoginsResponse( ).setLoginTypesList( logins );
  }

  @Override
  public LoginPropertiesResponse loginProperties(LoginPropertiesRequest request) {
    String login = request.getLoginType();
    out.println( "REQ: Login Properties( " + login + " )" );
    List<PropertyDef> props = new ArrayList<PropertyDef>( );
    if ( login.equals( MessageConstants.OPEN_LOGIN ) )
      ;
    else if ( login.equals( MessageConstants.USER_PWD_LOGIN ) ) {
      props.add( new PropertyDef( ).setName( "user-name" )
          .setType( PropertyType.STRING ) );
      props.add( new PropertyDef( ).setName( "password" )
          .setType( PropertyType.STRING ) );
    }
    else {
      throw new IllegalArgumentException( "Invalid login type: " + login );
    }
    return new LoginPropertiesResponse( )
        .setLoginType( login )
        .setPropertiesList( props );
  }

  @Override
  public void login(LoginRequest request) {
    out.print( "REQ: Login( " );
    String sep = "";
    for ( PropertyValue prop : request.getPropertiesList() ) {
      out.print( sep );
      out.print( prop.getName() );
      out.print( " = \"" );
      out.print( prop.getValue() );
      out.print( "\"" );
      sep = ", ";
    }
    out.println( " )" );
  }

  @Override
  public SuccessResponse executeStmt(ExecuteRequest request) {
    out.println( "REQ: Execute Stmt: " + request.getStatement() );
    return new SuccessResponse( )
        .setRowCount( 10 );
  }

  @Override
  public void executeQuery(QueryRequest request) {
    out.println( "REQ: Execute Query: " + request.getStatement() );
    step = 0;
  }

  @Override
  public DataResponse requestData() {
    switch ( step ) {
    case 0:
      step = 1;
      return new DataResponse( new InformationResponse( )
          .setSqlCode( "00100" )
          .setCode( 20 )
          .setMessage( "Working" ) );
    case 1:
      List<ColumnSchema> cols = new ArrayList<>( );
      cols.add( new ColumnSchema( )
          .setName( "first" )
          .setCardinality( Cardinality.OPTIONAL.cardinalityCode() )
          .setType( DataType.STRING.typeCode() ) );
      cols.add( new ColumnSchema( )
          .setName( "second" )
          .setCardinality( Cardinality.REQUIRED.cardinalityCode() )
          .setType( DataType.STRING.typeCode() ) );
      step = 2;
      return new DataResponse( new SchemaResponse( )
          .setColumnsList( cols ) );
    case 2:
      step = 3;
      return new DataResponse( "ABC|xyz".getBytes() );
    default:
      return new DataResponse( DataResponse.Type.EOF );
    }
  }

  @Override
  public void cancelQuery() {
    out.println( "REQ: Cancel Query" );
  }

  @Override
  public void goodbye() {
    out.println( "REQ: Goodbye" );
    out.flush( );
  }
}
