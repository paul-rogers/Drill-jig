package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ArrayValueAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.container.FieldValueContainer;
import org.apache.drill.jig.container.NullableFieldValueContainer;
import org.apache.drill.jig.container.SingleFieldValueContainer;
import org.apache.drill.jig.container.VariantFieldValueContainer;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.ArrayFieldValue;
import org.apache.drill.jig.types.ArrayValueImpl;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.MapFieldValue;
import org.apache.drill.jig.types.ArrayFieldValue.SimpleArrayValueAccessor;

/**
 * Definition of the a field used to build the field values that correspond
 * to a set of field definitions and accessors.
 */

public abstract class DataDef {
  
  public final DataType type;
  public final boolean nullable;
  public FieldValueContainer container;
  
  public DataDef( DataType type, boolean nullable ) {
    this.type = type;
    this.nullable = nullable;
  }
  
  public abstract void build( FieldValueFactory factory );
  
  protected FieldValueContainer makeTypedContainer( AbstractFieldValue value, FieldAccessor accessor ) {
    if ( nullable && type != DataType.NULL ) {
      NullableFieldValueContainer container = new NullableFieldValueContainer( value );
      container.bind( accessor );
      return container;
    } else {
      return new SingleFieldValueContainer( value );
    }
  }

  /**
   * Definition of a scalar field.
   */
  
  public static class ScalarDef extends DataDef {
    
    public FieldAccessor accessor;
    
    public ScalarDef(DataType type, boolean nullable, FieldAccessor accessor ) {
      super(type, nullable);
      this.accessor = accessor;
    }

    @Override
    public void build( FieldValueFactory factory ) {
      if ( type.isVariant() ) {
        container = new VariantFieldValueContainer( factory );
      } else {
        AbstractFieldValue value = factory.buildValue( type );
        container = makeTypedContainer( value, accessor );
      }
      if ( accessor != null )
        container.bind( accessor );
    }
  }
  
  /**
   * Definition of a scalar field.
   */
  
  public static class MapDef extends DataDef {
    
    public MapValueAccessor accessor;
    
    public MapDef( boolean nullable, MapValueAccessor accessor ) {
      super(DataType.MAP, nullable);
      this.accessor = accessor;
    }

    @Override
    public void build( FieldValueFactory factory ) {
      MapFieldValue value = new MapFieldValue( );
      container = makeTypedContainer( value, accessor );
      container.bind( accessor );
    }
  }
  
  public static class ListDef extends DataDef {
    public final DataDef member;
    public ArrayValueAccessor arrayValueAccessor;
    public ArrayAccessor arrayAccessor;
    public ArrayValue arrayValue;
    
    public ListDef( boolean nullable, DataDef member, ArrayAccessor accessor) {
      super(DataType.LIST, nullable);
      this.member = member;
      this.arrayAccessor = accessor;
    }

    @Override
    public void build( FieldValueFactory factory ) {
      member.build( factory );
      if ( arrayValueAccessor == null ) {
        if ( arrayValue == null ) {
          ArrayValueImpl impl = new ArrayValueImpl( member.type, member.nullable, member.container );
          impl.bind( arrayAccessor );
          arrayValue = impl;
        }
        arrayValueAccessor = new SimpleArrayValueAccessor( arrayAccessor, arrayValue );
      }
      ArrayFieldValue value = new ArrayFieldValue( );
      value.bind( arrayValueAccessor );
      container = makeTypedContainer( value, arrayValueAccessor );
    }
  }
}

