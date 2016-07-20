package org.apache.drill.jig.serde;

public class Constants
{
//  public static final int OPTIONAL = 0;
//  public static final int REQUIRED = 1;
//  public static final int REPEATED = 2;
  
//  public static final int NULL_TYPE = 0;
//  public static final int ANY_TYPE = 1;
//  public static final int BOOLEAN_TYPE = 2;
//  public static final int LONG_TYPE = 3;
//  public static final int DOUBLE_TYPE = 4;
//  public static final int BIG_DECIMAL_TYPE = 5;
//  public static final int STRING_TYPE = 6;
//  
//  public static final int TYPE_COUNT = 7;
  
  
//  public static DataType codeToTypeMap[] = initCodeToTypeMap( );
//  public static Cardinality codeToCardinalityMap[] = initCodeToCardinalityMap( );
//  public static byte typeToCodeMap[] = initTypeToCodeMap( );
//  public static byte cardinalityToCodeMap[] = initCardinalityToCodeMap( );
//  public static int fieldTypeLengths[] = initTypeLengths( );

  private static byte ONE = 1;
  public static byte encode( boolean bool ) { return bool ? ONE : 0; }
  
  public static boolean decode( int flag ) {
    return flag != 0;
  }
  
//  private static byte[] initTypeToCodeMap() {
//    int count = DataType.values().length;
//    byte map[] = new byte[ count ];
//    map[ DataType.BOOLEAN.ordinal() ] = Constants.BOOLEAN_TYPE;
//    map[ DataType.INT64.ordinal() ] = Constants.LONG_TYPE;
//    map[ DataType.FLOAT64.ordinal() ] = Constants.DOUBLE_TYPE;
//    map[ DataType.DECIMAL.ordinal() ] = Constants.BIG_DECIMAL_TYPE;
//    map[ DataType.STRING.ordinal() ] = Constants.STRING_TYPE;
//    map[ DataType.ANY.ordinal() ] = Constants.ANY_TYPE;
//    return map;
//  }

//  private static byte[] initCardinalityToCodeMap() {
//    int count = Cardinality.values().length;
//    byte map[] = new byte[ count ];
//    map[ Cardinality.OPTIONAL.ordinal() ] = Constants.OPTIONAL;
//    map[ Cardinality.REQUIRED.ordinal() ] = Constants.REQUIRED;
//    map[ Cardinality.REPEATED.ordinal() ] = Constants.REPEATED;
//    return map;
//  }

//  private static DataType[] initCodeToTypeMap() {
//    int count = DataType.values().length;
//    DataType types[] = new DataType[ count ];
//    types[ Constants.BOOLEAN_TYPE ] = DataType.BOOLEAN;
//    types[ Constants.LONG_TYPE ] = DataType.INT64;
//    types[ Constants.DOUBLE_TYPE ] = DataType.FLOAT64;
//    types[ Constants.BIG_DECIMAL_TYPE ] = DataType.DECIMAL;
//    types[ Constants.STRING_TYPE ] = DataType.STRING;
//    types[ Constants.ANY_TYPE ] = DataType.ANY;
//    return types;
//  }
//
//  private static Cardinality[] initCodeToCardinalityMap() {
//    int count = Cardinality.values().length;
//    Cardinality map[] = new Cardinality[ count ];
//    map[ Constants.OPTIONAL ] = Cardinality.OPTIONAL;
//    map[ Constants.REQUIRED ] = Cardinality.REQUIRED;
//    map[ Constants.REPEATED ] = Cardinality.REPEATED;
//    return map;
//  }

//  private static int[] initTypeLengths() {
//    int lengths[] = new int[ Constants.TYPE_COUNT ];
//    lengths[ Constants.BOOLEAN_TYPE ] = 1;
//    lengths[ Constants.LONG_TYPE ] = ENCODED_LONG;
//    lengths[ Constants.DOUBLE_TYPE ] = ENCODED_LONG;
//    lengths[ Constants.BIG_DECIMAL_TYPE ] = LENGTH_AND_VALUE;
//    lengths[ Constants.STRING_TYPE ] = LENGTH_AND_VALUE;
//    lengths[ Constants.ANY_TYPE ] = TYPE_AND_VALUE;
//    return lengths;
//  }

}
