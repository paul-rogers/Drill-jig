package org.apache.drill.jig.exception;

public class ValueConversionError extends RuntimeException
{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public ValueConversionError(String msg) {
    super( msg );
  }

  public ValueConversionError(String msg,Exception e) {
    super( msg, e );
  }

}
