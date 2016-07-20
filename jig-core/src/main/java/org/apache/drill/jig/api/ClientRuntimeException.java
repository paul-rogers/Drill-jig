package org.apache.drill.jig.api;

public class ClientRuntimeException extends RuntimeException
{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public ClientRuntimeException(String msg) {
    super( msg );
  }


  public ClientRuntimeException(String msg, Exception e) {
    super( msg, e );
  }
}
