package org.apache.drill.jig.api;

import java.nio.charset.Charset;

public class Constants
{
  /**
   * Data is encoded as:<br>
   * <pre>[size | body]</pre><br>
   * Where size is an encoded 32-bit integer.
   */
  
  public static final int LENGTH_AND_VALUE = -1;
  
  /**
   * Value is stored as an encoded (compressed)
   * long.
   */
  
  public static final int ENCODED_LONG = -2;
  
  /**
   * Value is encoded as:<br>
   * <pre>[type | data]</pre><br>
   * Where type is a single-byte type code and data
   * is encoded as for the indicated type.
   */
  
  public static final int TYPE_AND_VALUE = -3;
  
  /**
   * Data is encoded as:<br>
   * <pre>[size | body]</pre><br>
   * Where size is a 32-bit integer.
   */
  
  public static final int BLOCK_LENGTH_AND_VALUE = -4;
  public static final int NOT_IMPLEMENTED = -5;

  public static Charset utf8Charset = Charset.forName("UTF-8");
}
