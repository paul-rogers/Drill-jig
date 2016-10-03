package org.apache.drill.jig.types;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.ArrayFieldValue.SimpleArrayValueAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.ArrayValueImpl.TypedArrayValue;
import org.apache.drill.jig.types.ArrayValueImpl.VariantArrayValue;

public class FieldBuilder {
  
  public static FieldValueContainer buildTypedContainer( boolean nullable, FieldAccessor accessor, AbstractFieldValue value ) {
    if ( nullable ) {
      return new NullableFieldValueContainer( accessor, value );
    } else {
      return new SingleFieldValueContainer( value );
    }    
  }

  public static ArrayFieldValue buildArrayFieldValue( ArrayAccessor arrayAccessor, ArrayValue arrayValue ) {
    ArrayFieldValue value = new ArrayFieldValue( );
    value.bind( new SimpleArrayValueAccessor( arrayAccessor, arrayValue ) );
    return value;
  }
  
//  public static ArrayValueImpl buildArrayValue( ArrayAccessor arrayAccessor, DataType memberType, FieldValueFactory factory ) {
//    if ( memberType.isVariant( ) ) {
//      return new VariantArrayValue( arrayAccessor, memberType, factory );
//    } else {
//      return new TypedArrayValue( arrayAccessor, memberType );
//    }
//  }
  
}
