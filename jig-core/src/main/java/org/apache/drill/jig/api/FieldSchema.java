package org.apache.drill.jig.api;

/**
 * The schema of a single field.
 */

public interface FieldSchema
{
//  enum Type
//  {
//    BOOLEAN( true, "boolean" ),
//    LONG( true, "long" ),
//    DOUBLE( true, "double" ),
//    BIG_DECIMAL( true, "BigDecimal" ),
//    STRING( true, "String" ),
//    NULL( true, "null" ),
//    ANY( false, "any" ),
//    TUPLE( false, "Tuple" );
//    
//    private boolean isScalar;
//    private String displayName;
//    
//    private Type( boolean isScalar, String name ) {
//      this.isScalar = isScalar;
//      displayName = name;
//    }
//    
//    public String getDisplayName( ) { return displayName; }
//    public boolean isScalar( ) { return isScalar; }
//    public boolean isTuple( ) { return this == TUPLE; }
//    public boolean isCompatible(Type dataType) {
//      if ( this == ANY ) {
//        return true;
//      }
//      return this == dataType;
//    }   
//  }
  
  String getName( );
  int getIndex();
  DataType getType( );
  Cardinality getCardinality( );  
//    TupleSchema getStructure( );
  String getDisplayType();
  int getLength();
}