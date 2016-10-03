package org.apache.drill.jig.types;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.BoxedAccessor.VariantBoxedAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayValueAccessor;
import org.apache.drill.jig.types.FieldAccessor.TypeAccessor;
import org.apache.drill.jig.types.ArrayFieldValue.SimpleArrayValueAccessor;

public abstract class DataDef {
  public final DataType type;
  public final boolean nullable;
  public FieldValueContainer container;
  
  public DataDef( DataType type, boolean nullable ) {
    this.type = type;
    this.nullable = nullable;
  }
  
  public abstract void build( FieldValueFactory factory );
  
  protected FieldValueContainer makeTypedContainer( AbstractFieldValue value ) {
    if ( nullable ) {
      return new NullableFieldValueContainer( value );
    } else {
      return new SingleFieldValueContainer( value );
    }
  }

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
        container = makeTypedContainer( value );
      }
      if ( accessor != null )
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
          ArrayValueImpl impl = new ArrayValueImpl( member.type, member.container );
          impl.bind( arrayAccessor );
          arrayValue = impl;
        }
        arrayValueAccessor = new SimpleArrayValueAccessor( arrayAccessor, arrayValue );
      }
      ArrayFieldValue value = new ArrayFieldValue( );
      value.bind( arrayValueAccessor );
      container = makeTypedContainer( value );
    }
  }
}

