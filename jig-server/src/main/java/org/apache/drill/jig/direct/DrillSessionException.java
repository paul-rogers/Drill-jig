package org.apache.drill.jig.direct;

import org.apache.drill.jig.exception.JigException;

public class DrillSessionException extends JigException
{
  private static final long serialVersionUID = 1L;
  
  public DrillSessionException(String msg) {
    super( msg );
  }
  
  public DrillSessionException(String msg, Exception e) {
    super( msg, e );
  }
}