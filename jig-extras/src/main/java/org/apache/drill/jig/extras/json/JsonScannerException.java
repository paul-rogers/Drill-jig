package org.apache.drill.jig.extras.json;

import org.apache.drill.jig.exception.ClientRuntimeException;

public class JsonScannerException extends ClientRuntimeException
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public JsonScannerException(String msg) {
    super( msg );
  }

  public JsonScannerException(String msg, Exception e) {
    super( msg, e );
  }
}
