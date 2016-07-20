package org.apache.drill.jig.util;

public class JigUtilities {

  public static boolean isBlank( String str ) {
    return str == null ||  str.trim().isEmpty();
  }

}
