package org.apache.drill.jig.api.json;

import org.apache.drill.jig.api.ClientRuntimeException;

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
