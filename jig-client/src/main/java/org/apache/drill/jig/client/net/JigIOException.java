package org.apache.drill.jig.client.net;

import java.io.IOException;

import org.apache.drill.jig.api.JigException;

public class JigIOException extends JigException
{
  private static final long serialVersionUID = 1L;
  
  public JigIOException(String msg, Exception e) {
    super( msg, e );
  }

  public JigIOException(IOException e) {
    super( e.getMessage(), e );
  }

}