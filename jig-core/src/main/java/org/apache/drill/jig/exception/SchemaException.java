package org.apache.drill.jig.exception;


public class SchemaException extends ClientRuntimeException
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public SchemaException(String msg) {
    super( msg );
  }

  public SchemaException(String msg, Exception e) {
    super( msg, e );
  }
}
