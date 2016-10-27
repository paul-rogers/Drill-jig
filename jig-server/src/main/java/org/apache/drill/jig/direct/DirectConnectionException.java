package org.apache.drill.jig.direct;

import org.apache.drill.jig.exception.JigException;

public class DirectConnectionException extends JigException
{
  private static final long serialVersionUID = 1L;
  
  public DirectConnectionException(String msg) {
    super( msg );
  }
  
  public DirectConnectionException(String msg, Exception e) {
    super( msg, e );
  }
}