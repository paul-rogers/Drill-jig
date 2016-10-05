package org.apache.drill.jig.api.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.MapValue;

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

  public static void compareMaps(Object object, MapValue actual) {
    @SuppressWarnings("unchecked")
    Map<String,Object> map = (Map<String,Object>) object;
    assertEquals( map.size( ), actual.size( ) );
    for ( String key : map.keySet() ) {
      Object expectedValue = map.get( key );
      if ( expectedValue == null ) {
        assertTrue( actual.get( key ).isNull() );
      } else {
        assertFalse( actual.get( key ).isNull() );
        assertEquals( expectedValue, actual.get( key ).getValue() );
      }
    }
  }

}
