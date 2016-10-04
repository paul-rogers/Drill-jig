package org.apache.drill.jig.api.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Array;
import java.util.List;

import org.apache.drill.jig.api.ArrayValue;

public class Helpers {

  public static void compareLists( List<Object> expected, ArrayValue actual ) {
    assertEquals( expected.size(), actual.size() );
    for ( int i = 0;  i < expected.size( );  i++ ) {
      Object expectedVal = expected.get( i );
      if ( expectedVal == null ) {
        assertTrue( actual.get( i ).isNull() );
      } else {
        assertFalse( actual.get( i ).isNull() );
        assertEquals( expectedVal, actual.get( i ).getValue() );
      }
    }
  }

  public static void compareArrays( Object expected, ArrayValue actual ) {
    assertEquals( Array.getLength( expected ), actual.size() );
    for ( int i = 0;  i <  Array.getLength( expected );  i++ ) {
      Object expectedVal = Array.get( expected, i );
      if ( expectedVal == null ) {
        assertTrue( actual.get( i ).isNull() );
      } else {
        assertFalse( actual.get( i ).isNull() );
        assertEquals( expectedVal, actual.get( i ).getValue() );
      }
    }
  }

}
