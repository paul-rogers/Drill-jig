package org.apache.drill.jig.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.drill.jig.api.DrillConnection;
import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.client.net.JigClientFacade;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesResponse;
import org.apache.drill.jig.proto.LoginRequest;
import org.apache.drill.jig.proto.PropertyDef;
import org.apache.drill.jig.proto.PropertyValue;
import org.apache.drill.jig.protocol.MessageConstants;

/**
 * Get information about login options, then log in using
 * one of the options.
 */

public class DrillLoginFactory
{
  private JigClientFacade client;
  private ConnectionFactory factory;
  
  public DrillLoginFactory(ConnectionFactory connectionFactory) {
    this.client = connectionFactory.client;
    this.factory = connectionFactory;
  }
  
  /**
   * Get a list of login methods supported by the Drill cluster.
   * 
   * @return
   * @throws JigException
   */
  public List<String> getLoginMethods( ) throws JigException {
    ListLoginsResponse resp = client.getLoginMethods();
    return resp.getLoginTypesList();
  }
  
  /**
   * Get a list of the properties required for the selected login
   * method.
   * 
   * @param method
   * @return
   * @throws JigException
   */
  public List<PropertyDef> getLoginProperties( String method ) throws JigException {
    LoginPropertiesResponse resp = client.getLoginProperties( MessageConstants.USER_PWD_LOGIN );
    assert MessageConstants.USER_PWD_LOGIN == resp.getLoginType();
    return resp.getPropertiesList();
  }
  
  /**
   * General login given a map of key/value pairs where each key is
   * a property defined by the login method, and the value is the
   * value for that property.
   * 
   * @param credentials
   * @return
   * @throws JigException
   */
  public DrillConnection login( Map<String,String> credentials ) throws JigException {
    LoginRequest req = new LoginRequest( )
        .setLoginType( MessageConstants.USER_PWD_LOGIN );
    List<PropertyValue> props = new ArrayList<>( );
    for ( String key : credentials.keySet() ) {
      PropertyValue prop = new PropertyValue( )
          .setName( key )
          .setValue( credentials.get( key ) );
      props.add( prop );
    }
    req.setPropertiesList( props );
    client.login( req );     
    return new RemoteConnection( factory );
  }
  
  /**
   * Simple user name/password login.
   * 
   * @param userName
   * @param password
   * @return
   * @throws JigException
   */
  public DrillConnection login( String userName, String password ) throws JigException {
    LoginRequest req = new LoginRequest( )
        .setLoginType( MessageConstants.USER_PWD_LOGIN );
    List<PropertyValue> props = new ArrayList<>( );
    PropertyValue prop = new PropertyValue( )
        .setName( MessageConstants.USER_NAME_PROPERTY )
        .setValue( userName );
    props.add( prop );
    prop = new PropertyValue( )
        .setName( MessageConstants.PASSWORD_PROPERTY )
        .setValue( password );
    props.add( prop );
    req.setPropertiesList( props );
    client.login( req );     
    return new RemoteConnection( factory );
  }
  
  /**
   * Login to an open server (that requires no authorization).
   * 
   * @return
   * @throws JigException
   */
  public DrillConnection login( ) throws JigException {
    LoginRequest req = new LoginRequest( )
        .setLoginType( MessageConstants.OPEN_LOGIN );
    client.login(req);    
    return new RemoteConnection( factory );
  }
}
