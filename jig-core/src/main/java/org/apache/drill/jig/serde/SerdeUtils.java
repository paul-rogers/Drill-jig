package org.apache.drill.jig.serde;

public class SerdeUtils
{
  private static byte ONE = 1;
  public static byte encode( boolean bool ) { return bool ? ONE : 0; }
  
  public static boolean decode( int flag ) {
    return flag != 0;
  }
}
