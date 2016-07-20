package org.apache.drill.jig.api;

public class ValueConversionError extends RuntimeException
{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public ValueConversionError(String msg) {
    super( msg );
  }

}
