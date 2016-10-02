package org.apache.drill.jig.extras.array;

import java.lang.reflect.Array;
import java.util.List;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.ArrayFieldSchemaImpl;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.extras.array.ArrayFieldHandle.ArrayTupleHandle;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.BoxedAccessor;
import org.apache.drill.jig.types.BoxedAccessor.VariantBoxedAccessor;
import org.apache.drill.jig.types.FieldValueContainer;
import org.apache.drill.jig.types.FieldValueContainerSet;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.NullableFieldValueContainer;
import org.apache.drill.jig.types.SingleFieldValueContainer;
import org.apache.drill.jig.types.VariantFieldValueContainer;

public class SchemaBuilder {
  public static class FieldImpl {
    public enum ListType {
      LIST, OBJECT_ARRAY, TYPED_OBJECT_ARRAY, PRIMITIVE_ARRAY,
      ARRAY_OF_ARRAY
    }

    int index;
    boolean nullable = false;
    DataType type;
    ListType listType;
    FieldImpl memberDef;

    public FieldImpl(int index) {
      this.index = index;
    }

    @SuppressWarnings("unchecked")
    public void define(Object object, FieldValueFactory factory) {
      nullable |= (object == null);
      if (!buildArray(object, factory)) {
        type = factory.mergeTypes(type, factory.objectToJigType(object));
        if (type == DataType.LIST) {
          buildList( (List<Object>) object, factory );
        }
      }
    }

    private boolean buildArray(Object object, FieldValueFactory factory) {
      if (object == null)
        return false;
      String objClass = object.getClass().getName();
      if (!objClass.startsWith("["))
        return false;
      if (type == null)
        type = DataType.LIST;
      else if (type != DataType.LIST)
        throw new ValueConversionError("Array incompatible with type " + type);

      // Figure out type

      ListType thisType;
      DataType memberType;
      char second = objClass.charAt( 1 );
      switch ( second ) {
      case '[':
        thisType = ListType.ARRAY_OF_ARRAY;
        memberType = DataType.LIST;
        break;
      case  'B':
        thisType = ListType.PRIMITIVE_ARRAY;
        memberType = DataType.INT8;
        break;
      case  'S':
        thisType = ListType.PRIMITIVE_ARRAY;
        memberType = DataType.INT16;
        break;
      case  'I':
        thisType = ListType.PRIMITIVE_ARRAY;
        memberType = DataType.INT32;
        break;
      case  'J':
        thisType = ListType.PRIMITIVE_ARRAY;
        memberType = DataType.INT64;
        break;
      case  'F':
        thisType = ListType.PRIMITIVE_ARRAY;
        memberType = DataType.FLOAT32;
        break;
      case  'D':
        thisType = ListType.PRIMITIVE_ARRAY;
        memberType = DataType.FLOAT64;
        break;
      case  'Z':
        thisType = ListType.PRIMITIVE_ARRAY;
        memberType = DataType.BOOLEAN;
        break;
      case 'L':
        thisType = ListType.OBJECT_ARRAY;
        int posn = objClass.indexOf(';');
        String memberClassName = objClass.substring( 2, posn );
        memberType = factory.classNameToJigType( memberClassName );
        break;
      default:
        throw new ValueConversionError( "Unknown Java array type: " + second );
      }

      if (listType == null)
        listType = thisType;
      else if (listType != thisType)
        throw new ValueConversionError("Incompatible arrays: " + listType
            + " and " + thisType);

     if (listType == ListType.OBJECT_ARRAY) {
        if ( memberDef == null )
          memberDef = new FieldImpl( 0 );
        int n = Array.getLength( object );
        for ( int i = 0;  i < n;  i++ ) {
          memberDef.define(Array.get(object, i), factory);
        }
      } else {
        if (memberDef == null) {
          memberDef = new FieldImpl(0);
          memberDef.type = memberType;
          memberDef.nullable = false;
        }
        else if ( memberDef.type != memberType ) {
          throw new ValueConversionError( "Cannot mix primitive array types: " + memberDef.type + " and " + memberType );
        }
      }
      return true;
    }

    private void buildList(List<Object> list, FieldValueFactory factory) {
      if ( memberDef == null )
        memberDef = new FieldImpl( 0 );
      int n = list.size( );
      for ( int i = 0;  i < n;  i++ ) {
        memberDef.define(list.get( i ), factory);
      }
    }

    public FieldSchemaImpl buildSchema(String name) {
      if (type == DataType.LIST)
        return new ArrayFieldSchemaImpl(name, nullable, memberDef.buildSchema(null));
      else
        return new FieldSchemaImpl(name, type, nullable);
    }

    public FieldValueContainer buildValue(FieldValueFactory factory, ArrayTupleHandle tupleHandle) {
      if ( listType != null ) {
        assert false;
      } else if ( type == DataType.VARIANT ) {
        ArrayFieldHandle accessor = new ArrayFieldHandle( tupleHandle, index );
        VariantBoxedAccessor valueAccessor = new VariantBoxedAccessor( accessor, factory );
        return new VariantFieldValueContainer( valueAccessor, factory );
      } else {
        AbstractFieldValue value = factory.buildValue( type );
        ArrayFieldHandle accessor = new ArrayFieldHandle( tupleHandle, index );
        value.bind( new BoxedAccessor( accessor ) );
        if ( nullable ) {
          return new NullableFieldValueContainer( accessor, value );
        } else {
          return new SingleFieldValueContainer( value );
        }
      }
      return null;
    }
  }

  FieldValueFactory factory;
  private FieldImpl fields[];

  public SchemaBuilder(FieldValueFactory factory ) {
    this.factory = factory;
  }

  public TupleSchema buildSchema(Batch batch ) {
    int fieldCount = batch.names.length;
    fields = new FieldImpl[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      fields[i] = new FieldImpl(i);
    }
    int rowCount = batch.data.length;
    for (int i = 0; i < rowCount; i++) {
      for (int j = 0; j < fieldCount; j++) {
        fields[j].define(batch.data[i][j], factory);
      }
    }
    TupleSchemaImpl schema = new TupleSchemaImpl();
    for (int i = 0; i < fieldCount; i++) {
      schema.add(fields[i].buildSchema(batch.names[i]));
    }
    return schema;
  }
  
  public FieldValueContainerSet fieldValues( ArrayTupleHandle tupleHandle ) {
    int fieldCount = fields.length;
    FieldValueContainer values[] = new FieldValueContainer[ fieldCount ];
    for ( int i = 0;  i < fieldCount;  i++ ) {
      values[i] = fields[i].buildValue( factory, tupleHandle );
    }
    return new FieldValueContainerSet( values );
  }
}
