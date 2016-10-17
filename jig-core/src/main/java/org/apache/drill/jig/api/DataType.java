package org.apache.drill.jig.api;

public enum DataType 
{
  /**
   * Undefined means that, for whatever reason, we cannot infer the
   * type of the field. This can occur, say, for the member type of
   * an empty list.
   */
  
  UNDEFINED( 0, false, "Undefined", 0, false ),
  
  /**
   * The null type means that we tried to infer the field type,
   * but every occurrence of the field contained a null value.
   */
  
  NULL( 1, true, "Null", 0, false ), 
  BOOLEAN( 2, true, "Boolean", 1, true ), 
  INT8( 3, true, "Int-8", 1, true ),
  INT16( 4, true, "Int-16", 2, true ),
  INT32( 5, true, "Int-32", Constants.ENCODED_LONG, true ),
  INT64( 6, true, "Int-64", Constants.ENCODED_LONG, true ), 
  FLOAT32( 7, true, "Float-32", 4, true ), 
  FLOAT64( 8, true, "Float-64", 8, true ), 
  DECIMAL( 9, true, "Decimal", Constants.LENGTH_AND_VALUE, false ), 
  STRING( 10, true, "String", Constants.LENGTH_AND_VALUE, false ), 
  BLOB( 11, true, "BLOB", Constants.LENGTH_AND_VALUE, false ), 
  DATE( 12, true, "Date", Constants.NOT_IMPLEMENTED, false ), 
  LOCAL_DATE_TIME( 13, true, "Local-Date-Time", Constants.NOT_IMPLEMENTED, false ), 
  UTC_DATE_TIME( 14, true, "UTC-Date-Time", Constants.NOT_IMPLEMENTED, false ), 
  DATE_TIME_SPAN( 15, true, "Date-Time-Span", Constants.NOT_IMPLEMENTED, false ), 
  LIST( 16, false, "List", Constants.BLOCK_LENGTH_AND_VALUE, false ),
  MAP( 17, false, "Map", Constants.BLOCK_LENGTH_AND_VALUE, false ),
  
  /**
   * Represents a field that can take on any scalar value.
   */
  
  VARIANT( 18, true, "Any", Constants.TYPE_AND_VALUE, false ),
  
  /**
   * Represents a variant field that can take on only numeric
   * values (as in JSON).
   */
  
  NUMBER( 19, true, "Number", Constants.TYPE_AND_VALUE, false ),
  
  /**
   * Represents a tuple field (with specified schema.)
   */
  
  TUPLE( 20, false, "Tuple", Constants.NOT_IMPLEMENTED, false );
  
  private static DataType codeToTypeMap[] = initCodeToTypeMap( );
  public static int lengthForCode[] = initLengthForCodeMap( );

  private int typeCode;
  private String displayName;
  private boolean isScalar;
  private int storageLength;
  private boolean isPrimitive;

  private DataType( int typeCode, boolean isScalar, String displayName, int storageLen, boolean isPrimitive ) {
    this.typeCode = typeCode;
    this.displayName = displayName;
    this.isScalar = isScalar;
    this.storageLength = storageLen;
    this.isPrimitive = isPrimitive;
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
  public String displayName( ) { return displayName; }
  public boolean isScalar( ) { return isScalar; }
  public boolean isPrimitive( ) { return isPrimitive; }
  
  /**
   * Returns either the fixed storage length, a code that describes
   * the variable-length format. This is the length used to serialize
   * the data in the record wire protocol.
   * 
   * @return
   */
  
  public int getStorageLength( ) { return storageLength; }
  
  public boolean isCompatible(DataType dataType) {
    if ( this == VARIANT ) {
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

  public boolean isVariant() {
    return this == VARIANT  ||  this == NUMBER;
  }

  public boolean isUndefined() {
    return this == NULL  ||  this == UNDEFINED;
  }

}
