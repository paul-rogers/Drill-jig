package org.apache.drill.jig.client.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public class NetworkClient implements AutoCloseable
{
  private Socket socket;
  private String host;
  private int port;

  public NetworkClient( String host, int port ) {
    this.host = host;
    this.port = port;
  }
  
  public void connect( ) throws IOException {
    socket = new Socket( );
    int timeout = 30_000;
    SocketAddress endpoint = new InetSocketAddress( host, port );
    socket.connect(endpoint, timeout);
  }
  
  public void write( ByteBuffer buf ) throws IOException {
    write( buf.array(), buf.position() );
  }
  
  public void write( byte buf[], int len ) throws IOException {
    socket.getOutputStream().write( buf, 0, len );
  }
  
  public void flush( ) throws IOException {
    socket.getOutputStream().flush();
  }
  
  public void read( ByteBuffer buf, int len ) throws IOException {
    read( buf.array(), len );
    buf.limit( len );
  }
  
  public void read( byte buf[], int len ) throws IOException {
    socket.getInputStream().read( buf, 0, len );
  }

  @Override
  public void close() throws Exception {
    if ( socket != null ) {
      socket.close( );
      socket = null;
    }
  }
}