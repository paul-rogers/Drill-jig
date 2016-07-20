package org.apache.drill.jig.api;

/**
 * Describes the cardinality of a field (what Drill calls the field's
 * "mode".)
 */

public enum Cardinality
{
  /**
   * Cardinality [0,1] or 1?. Equivalent to a nullable scalar.
   */
  
  OPTIONAL( 0, "?" ),
  
  /**
   * Cardinality of 1. Equivalent to a non-nullable scalar.
   */
  
  REQUIRED( 1,"" ),
  
  /**
   * Cardinality of [0,n] or *. Equivalent to a (possibly-zero length)
   * array. Note that Drill (and thus jig) does not differentiate between
   * a null array and a zero-length array.
   */
  
  REPEATED( 2, "[]" );

  private static Cardinality codeToCardinalityMap[] = initCodeToCardinalityMap( );

  private int code;
  private String displaySuffix;
  
  private Cardinality( int code, String suffix ) {
    this.code = code;
    displaySuffix = suffix;
  }
  
  /**
   * The code is fixed, independent of the ordinal code. While the ordinal code
   * will change based on the syntax of this enum (declaration order), the
   * code is defined as part of the public, external, versioned API and must
   * be fixed permanently.
   * 
   * @return
   */
  public int cardinalityCode( ) { return code; }    
  public String getDisplaySuffix( ) { return displaySuffix; }
  
  private static Cardinality[] initCodeToCardinalityMap() {
    Cardinality cardinalities[] = new Cardinality[ values().length ];
    for ( Cardinality cardinality : values( ) ) {
      cardinalities[ cardinality.code ] = cardinality;
    }
    return cardinalities;
  }
  
  public static Cardinality cardinalityForCode( int code ) {
    if ( code < 0  ||  code >= codeToCardinalityMap.length )
      return null;
    return codeToCardinalityMap[ code ];
  }
}