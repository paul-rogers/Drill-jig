package org.apache.drill.jig.direct;

public class DirectConnectionError extends RuntimeException
{
  private static final long serialVersionUID = 1L;
  
  public DirectConnectionError(String msg) {
    super( msg );
  }

}