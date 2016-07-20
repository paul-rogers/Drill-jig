package org.apache.drill.jig.api;

public enum DataType 
{
  UNDEFINED( 0, false, "Undefined", 0 ),
  NULL( 1, true, "Null", 0 ), 
  BOOLEAN( 2, true, "Boolean", 1 ), 
  INT8( 3, true, "Int-8", 1 ),
  INT16( 4, true, "Int-16", 2 ),
  INT32( 5, true, "Int-32", Constants.ENCODED_LONG ),
  INT64( 6, true, "Int-64", Constants.ENCODED_LONG ), 
  FLOAT32( 7, true, "Float-32", 4 ), 
  FLOAT64( 8, true, "Float-64", 8 ), 
  DECIMAL( 9, true, "Decimal", Constants.LENGTH_AND_VALUE ), 
  STRING( 10, true, "String", Constants.LENGTH_AND_VALUE ), 
  BLOB( 11, true, "BLOB", Constants.LENGTH_AND_VALUE ), 
  DATE( 12, true, "Date", Constants.NOT_IMPLEMENTED ), 
  LOCAL_DATE_TIME( 13, true, "Local-Date-Time", Constants.NOT_IMPLEMENTED ), 
  UTC_DATE_TIME( 14, true, "UTC-Date-Time", Constants.NOT_IMPLEMENTED ), 
  DATE_TIME_SPAN( 15, true, "Date-Time-Span", Constants.NOT_IMPLEMENTED ), 
  LIST( 16, false, "List", Constants.NOT_IMPLEMENTED ),
  MAP( 17, false, "Map", Constants.NOT_IMPLEMENTED ),
  ANY( 18, true, "Any", Constants.TYPE_AND_VALUE );
  
  private static DataType codeToTypeMap[] = initCodeToTypeMap( );
  public static int lengthForCode[] = initLengthForCodeMap( );

  private int typeCode;
  private String displayName;
  private boolean isScalar;
  private int storageLength;

  private DataType( int typeCode, boolean isScalar, String displayName, int storageLen ) {
    this.typeCode = typeCode;
    this.displayName = displayName;
    this.isScalar = isScalar;
    this.storageLength = storageLen;
  }
  
  /**
   * The code is fixed, independent of the ordinal code. While the ordinal code
   * will change based on the syntax of this enum (declaration order), the
   * code is defined as part of the public, external, versioned API and must
   * be fixed permanently.
   * 
   * @return
   */
  public int typeCode( ) { return typeCode; }
  public String getDisplayName( ) { return displayName; }
  public boolean isScalar( ) { return isScalar; }
  
  /**
   * Returns either the fixed storage length, a code that describes
   * the variable-length format. This is the length used to serialize
   * the data in the record wire protocol.
   * 
   * @return
   */
  
  public int getStorageLength( ) { return storageLength; }
  
  public boolean isCompatible(DataType dataType) {
    if ( this == ANY ) {
      return true;
    }
    return this == dataType;
  }
  
  private static DataType[] initCodeToTypeMap() {
    DataType types[] = new DataType[ values().length ];
    for ( DataType type : values( ) ) {
      types[ type.typeCode ] = type;
    }
    return types;
  }
  
  public static DataType typeForCode( int code ) {
    if ( code < 0  ||  code >= codeToTypeMap.length )
      return null;
    return codeToTypeMap[ code ];
  }

  private static int[] initLengthForCodeMap() {
    int lengths[] = new int[ values( ).length ];
    for ( DataType type : values( ) ) {
      lengths[ type.typeCode ] = type.storageLength;
    }
    return lengths;
  }

}
