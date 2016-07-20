package org.apache.drill.jig.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

public class CompareFiles {

  public static boolean compare( File first, File second ) {
    try {
      return compare( new FileReader( first ), new FileReader( second ) );
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return false;
    }
  }
  
  public static boolean compareResource( String resourceName, String actual ) {
    InputStream is = CompareFiles.class.getResourceAsStream( resourceName );
    if ( is == null )
      throw new IllegalStateException( "Resource not found: " + resourceName );
    try {
      return compare( new InputStreamReader( is, "UTF-8" ),
                      new StringReader( actual ) );
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return false;
    }
  }

  private static boolean compare( Reader first, Reader second ) {
    BufferedReader in1 = new BufferedReader( first );
    BufferedReader in2 = new BufferedReader( second );
    boolean ok = false;
    try {
      int lineNo = 0;
      for ( ; ; ) {
        lineNo++;
        String line1;
        for ( ; ; ) {
          line1 = in1.readLine();
          if ( line1 == null )
            break;
          line1 = line1.trim();
          if ( line1.isEmpty() || line1.startsWith( "#" ) )
            continue;
          break;
        }
        String line2;
        for ( ; ; ) {
          line2 = in2.readLine();
          if ( line2 == null )
            break;
          line2 = line2.trim();
          if ( line2.isEmpty() )
            continue;
          break;
        }
        if ( line1 == null  &&  line2 == null ) {
          ok = true;
          break;
        }
        if ( line1 == null  ||  line2 == null ) {
          System.err.println( "Unexpected eof on line " + lineNo );
          break;
        }
        
        if ( ! line1.equals( line2 ) ) {
          System.err.println( "Mismatch on line " + lineNo );
          break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    finally {
      try {
        in1.close( );
      } catch (IOException e) {
        // Ignore
      }
      try {
        in2.close( );
      } catch (IOException e) {
        // Ignore
      }
    }
    return ok;
  }
}
