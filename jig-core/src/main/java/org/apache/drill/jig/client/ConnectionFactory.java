package org.apache.drill.jig.client;

import org.apache.drill.jig.client.net.JigClientFacade;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.protocol.HelloRequest;
import org.apache.drill.jig.protocol.HelloResponse;
import org.apache.drill.jig.protocol.MessageConstants;

public class ConnectionFactory
{
  private String host = MessageConstants.DEFAULT_HOST;
  private int port = MessageConstants.DEFAULT_PORT;
  protected int bufferSize = MessageConstants.DEFAULT_RESULTS_BUFFER_SIZE_K * 1024;
  protected HelloResponse helloResponse;
  protected int timeout = MessageConstants.DEFAULT_QUERY_TIMEOUT_SECS;
  protected JigClientFacade client;
  public int dataPollPeriodMs = MessageConstants.DEFAULT_QUERY_POLL_PERIOD_MS;
  
  public ConnectionFactory toHost( String hostName ) {
    host = hostName;
    return this;
  }
  
  public ConnectionFactory onPort( int portNo ) {
    port = portNo;
    return this;
  }
  
  public ConnectionFactory withBufferSize( int bufferSize ) {
    this.bufferSize = bufferSize;
    return this;
  }
  
  public ConnectionFactory withTimeoutSec( int timeout ) {
    this.timeout = timeout;
    return this;
  }
  
  public ConnectionFactory withQueryPollPeriodMs( int periodMs ) {
    dataPollPeriodMs = periodMs;
    return this;
  }
  
  public DrillLoginFactory connect( ) throws JigException {
    client = new JigClientFacade( );
    client.connect( host, port );
    HelloRequest req = new HelloRequest( ClientConstants.CLIENT_API_VERSION, ClientConstants.CLIENT_API_VERSION );
    helloResponse = client.hello( req );
    return new DrillLoginFactory( this );
  }
}
