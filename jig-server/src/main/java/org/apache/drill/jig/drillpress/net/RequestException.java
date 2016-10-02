package org.apache.drill.jig.drillpress.net;

import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.protocol.MessageConstants;

public class RequestException extends JigException
{
  private static final long serialVersionUID = 1L;
  
  public final int errorCode;
  public String sqlCode;

  public static class InvalidRequestException extends RequestException
  {
    private static final long serialVersionUID = 1L;
    
    public InvalidRequestException( String msg ) {
      super( msg, MessageConstants.INVALID_REQUEST_ERROR );
      sqlCode = "TBD";
    }
  }
  
  public static class IncompatibleVersionsException extends RequestException
  {
    private static final long serialVersionUID = 1L;
    
    public IncompatibleVersionsException( String msg ) {
      super( msg, MessageConstants.INCOMPATIBLE_VERSIONS_ERROR );
      sqlCode = "TBD";
    }
  }
  
  public RequestException( String msg, int errorCode ) {
    super( msg );
    this.errorCode = errorCode;
  }
}
