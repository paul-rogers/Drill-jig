package org.apache.drill.jig.api;

import java.nio.charset.Charset;

public class Constants
{
  public static final int LENGTH_AND_VALUE = -1;
  public static final int ENCODED_LONG = -2;
  public static final int TYPE_AND_VALUE = -3;
  public static final int NOT_IMPLEMENTED = -4;

  public static Charset utf8Charset = Charset.forName("UTF-8");
}
