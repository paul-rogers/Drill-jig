package org.apache.drill.jig.drillpress;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.direct.DrillSession;
import org.apache.drill.jig.direct.DrillSessionException;
import org.apache.drill.jig.drillpress.net.RequestException;
import org.apache.drill.jig.drillpress.net.RequestProcessor;
import org.apache.drill.jig.drillpress.net.RequestException.IncompatibleVersionsException;
import org.apache.drill.jig.drillpress.net.RequestException.InvalidRequestException;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.proto.ExecuteRequest;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesRequest;
import org.apache.drill.jig.proto.LoginPropertiesResponse;
import org.apache.drill.jig.proto.LoginRequest;
import org.apache.drill.jig.proto.PropertyDef;
import org.apache.drill.jig.proto.PropertyType;
import org.apache.drill.jig.proto.PropertyValue;
import org.apache.drill.jig.proto.QueryRequest;
import org.apache.drill.jig.proto.SuccessResponse;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.protocol.HelloRequest;
import org.apache.drill.jig.protocol.HelloResponse;
import org.apache.drill.jig.protocol.MessageConstants;

public class SessionRequestProcessor implements RequestProcessor
{
  public enum SessionState { NEW, PRE_LOGIN, READY, QUERY, END };
  
  private SessionState state = SessionState.NEW;
  private DrillSession session;
  private QuerySerializer querySerializer;
  private DrillPressContext drillPressContext;
  
  public SessionRequestProcessor(DrillPressContext drillPressContext) {
    this.drillPressContext = drillPressContext;
  }

  @Override
  public HelloResponse hello(HelloRequest request) throws JigException {
    if ( state != SessionState.NEW ) {
      throw new InvalidRequestException( "Already exchanged versions." );
    }
    if ( request.clientVersion < DrillPressContext.LOWEST_SUPPORTED_VERSION  ||
         request.lowestClientVersion > DrillPressContext.SERVER_VERSION ) {
      state = SessionState.END;
      throw new IncompatibleVersionsException(
          "Server supprorts versions " + DrillPressContext.LOWEST_SUPPORTED_VERSION +
          " through " + DrillPressContext.SERVER_VERSION );
    }
    state = SessionState.PRE_LOGIN;
    return new HelloResponse(
        DrillPressContext.SERVER_VERSION,
        Math.min( request.clientVersion,
            DrillPressContext.SERVER_VERSION )
        );
  }

  @Override
  public ListLoginsResponse listLogins() throws JigException {
    assertPreLogin( );
    List<String> logins = new ArrayList<>( );
    logins.add( MessageConstants.OPEN_LOGIN );
//    logins.add( MessageConstants.USER_PWD_LOGIN );
    
    // TODO: Add others
    
    return new ListLoginsResponse( ).setLoginTypesList( logins );
  }

  @Override
  public LoginPropertiesResponse loginProperties(LoginPropertiesRequest request) throws JigException {
    assertPreLogin( );
    String login = request.getLoginType();
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
      throw new InvalidRequestException( "Invalid login type: " + login );
    }
    return new LoginPropertiesResponse( )
        .setLoginType( login )
        .setPropertiesList( props );
  }

  @Override
  public void login(LoginRequest request) throws JigException {
    assertPreLogin( );
    String loginType = request.getLoginType();
    if ( MessageConstants.OPEN_LOGIN.equals( loginType ) ) {
      ;
    }
    else if ( MessageConstants.USER_PWD_LOGIN.equals( loginType ) ) {
      List<PropertyValue> props = request.getPropertiesList();
      String userName = null;
      String pwd = null;
      for ( PropertyValue prop : props ) {
        if ( MessageConstants.USER_NAME_PROPERTY.equals( prop.getName() ) ) {
          userName = prop.getValue();
        }
        else if ( MessageConstants.PASSWORD_PROPERTY.equals( prop.getName() ) ) {
          pwd = prop.getValue();
        }
      }
      drillPressContext.withLogin( userName, pwd );
    } else {
      throw new InvalidRequestException( "Invalid login type: " + request.getLoginType() );
    }
    try {
      session = drillPressContext.connectToDrill();
    } catch (DrillSessionException e) {
      throw new RequestException( "Drill connect failed: " + e.getMessage(),
          MessageConstants.DRILL_CONNECT_ERROR );
    }
    state = SessionState.READY;
  }

  private void assertPreLogin() throws JigException {
    if ( state != SessionState.PRE_LOGIN )
      throw new InvalidRequestException( "Already logged in" );
  }

  @Override
  public SuccessResponse executeStmt(ExecuteRequest request) throws JigException {
    assertReady( );
    try {
      session.execute( request.getStatement() );
    } catch (JigException e) {
      throw new RequestException( e.getMessage(),
          MessageConstants.EXECUTE_ERROR );
    }
    return new SuccessResponse( );
  }

  @Override
  public void executeQuery(QueryRequest request) throws JigException {
    assertReady( );
    querySerializer = new QuerySerializer( request );
    querySerializer.start( session );
    state = SessionState.QUERY;
  }

  private void assertReady() throws JigException {
    if ( state == SessionState.PRE_LOGIN ) {
      throw new InvalidRequestException( "Not logged in" );
    }
    if ( state == SessionState.QUERY ) {
      throw new InvalidRequestException( "Already in a query" );
    }
    if ( state != SessionState.READY ) {
      throw new InvalidRequestException( "Not a valid session" );
    }
  }

  @Override
  public DataResponse requestData() throws JigException {
    assertInQuery( );
    DataResponse response = querySerializer.requestData();
    if ( response.type == DataResponse.Type.EOF ) {
      querySerializer.close();
      querySerializer = null;
      state = SessionState.READY;
    }
    return response;
  }

  @Override
  public void cancelQuery() throws JigException {
    if ( state != SessionState.QUERY )
      return;
    querySerializer.close();
    querySerializer = null;
    state = SessionState.READY;
  }

  private void assertInQuery() throws JigException {
    if ( state != SessionState.QUERY )
      throw new InvalidRequestException( "Not in a query" );
  }

  @Override
  public void goodbye() throws JigException {
    if ( querySerializer != null ) {
      querySerializer.close();
      querySerializer = null;
    }
    session.close();
    session = null;
    state = SessionState.END;
  }

}
