package org.apache.drill.jig.client.net;

import org.apache.drill.jig.api.JigException;

/**
 * Indicates an error response received from the server.
 * All server errors, in the Java implementation, are translated
 * to instances of this class and thrown. Member fields
 * provide access to the error code returned by the server.
 */

public class JigServerException extends JigException
{
  private static final long serialVersionUID = 1L;
  
  public final int errorCode;
  public final String sqlCode;
  
  public JigServerException(String message, int code, String sqlCode) {
    super( message == null ? "Server error " + code : message );
    errorCode = code;
    this.sqlCode = sqlCode;
  }

}