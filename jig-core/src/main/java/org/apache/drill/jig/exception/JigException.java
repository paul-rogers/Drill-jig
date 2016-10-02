package org.apache.drill.jig.exception;

/**
 * Base exception for exceptions likely caused by user code, or by
 * communication issues. These are errors that the application should
 * handle: either by recovering or changing application logic.
 * Subclasses provide more information about the kind of error and
 * are specific to the API implementation.
 */

public class JigException extends Exception
{
  private static final long serialVersionUID = 1L;

  public JigException(String msg) {
    super( msg );
  }
  
  public JigException(String msg, Exception e) {
    super( msg, e );
  }
}
