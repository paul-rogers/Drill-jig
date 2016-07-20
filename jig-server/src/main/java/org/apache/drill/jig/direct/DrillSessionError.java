package org.apache.drill.jig.direct;

public class DrillSessionError extends RuntimeException
{
  private static final long serialVersionUID = 1L;
  
  public DrillSessionError(String msg) {
    super( msg );
  }

}