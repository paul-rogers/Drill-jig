package org.apache.drill.jig.extras.array;

import java.lang.reflect.Array;
import java.util.List;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.ArrayFieldSchemaImpl;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.extras.array.ArrayFieldHandle.ArrayTupleHandle;
import org.apache.drill.jig.types.BoxedAccessor;
import org.apache.drill.jig.types.BoxedAccessor.VariantBoxedAccessor;
import org.apache.drill.jig.types.DataDef;
import org.apache.drill.jig.types.DataDef.ListDef;
import org.apache.drill.jig.types.DataDef.ScalarDef;
import org.apache.drill.jig.types.FieldAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldValueContainer;
import org.apache.drill.jig.types.FieldValueContainerSet;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.JavaArrayAccessor.ObjectArrayAccessor;
import org.apache.drill.jig.types.JavaArrayAccessor.PrimitiveArrayAccessor;
import org.apache.drill.jig.types.JavaListAccessor;

public class SchemaBuilder {
  public static class FieldImpl {
    public enum ListType {
      LIST, OBJECT_ARRAY, PRIMITIVE_ARRAY
    }

    int index;
    boolean nullable = false;
    DataType type;
    ListType listType;
    FieldImpl memberDef;
    private FieldAccessor accessor;

    public FieldImpl(int index) {
      this.index = index;
    }

    @SuppressWarnings("unchecked")
    public void define(Object object, FieldValueFactory factory) {
      nullable |= (object == null);
      if (!buildArray(object, factory)) {
        type = factory.mergeTypes(type, factory.objectToJigType(object));
        if (type == DataType.LIST) {
          listType = ListType.LIST;
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
        thisType = ListType.OBJECT_ARRAY;
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
        return new ArrayFieldSchemaImpl(name, nullable, memberDef.buildSchema("*"));
      else
        return new FieldSchemaImpl(name, type, nullable);
    }

    public void buildTupleFieldAccessor( ArrayTupleHandle tupleHandle, FieldValueFactory factory ) {
      ArrayFieldHandle fieldHandle = new ArrayFieldHandle( tupleHandle, index );
      buildFieldAccessor( fieldHandle, factory );
    }
    
    public void buildFieldAccessor( ObjectAccessor objAccessor, FieldValueFactory factory ) {
      if ( listType != null ) {
        buildListAccessor( objAccessor, factory );
      } else if ( type == DataType.MAP ) {
        buildMapAccessor( objAccessor, factory );
      } else if ( type.isVariant() ) {
        accessor = new VariantBoxedAccessor( objAccessor, factory );
      } else {
        accessor = new BoxedAccessor( objAccessor );
      }
    }

    private void buildListAccessor(ObjectAccessor objAccessor, FieldValueFactory factory) {
      ArrayAccessor arrayAccessor;
      switch ( listType ) {
      case LIST:
        arrayAccessor = new JavaListAccessor( objAccessor );
        break;
      case OBJECT_ARRAY:
        arrayAccessor = new ObjectArrayAccessor( objAccessor );
        break;
      case PRIMITIVE_ARRAY:
        arrayAccessor = new PrimitiveArrayAccessor( objAccessor, memberDef.type );
        break;
      default:
        throw new IllegalStateException( "Undefined list type: " + listType );
      }
      accessor = arrayAccessor;
      memberDef.buildFieldAccessor( (ObjectAccessor) arrayAccessor.memberAccessor(), factory );
    }

    private Object buildMapAccessor(FieldAccessor accessor2,
        FieldValueFactory factory) {
      // TODO Auto-generated method stub
      return null;
    }

    
    public DataDef buildDef( ) {
      if ( listType != null ) {
        return new ListDef( nullable, memberDef.buildDef( ), (ArrayAccessor) accessor );
      } else if ( type == DataType.MAP ) {
        assert false;
        return null;
      } else {
        return new ScalarDef( type, nullable, accessor );
      }
    }

    public FieldValueContainer build(ArrayTupleHandle tupleHandle,
        FieldValueFactory factory) {
      buildTupleFieldAccessor( tupleHandle, factory );
      DataDef def = buildDef( );
      def.build( factory );
      return def.container;
    }
    
//    private DataDef buildListDef( ObjectAccessor accessor) {
//     ArrayAccessor arrayAccessor;
//      switch ( listType ) {
//      case LIST:
//        arrayAccessor = new JavaListAccessor( accessor );
//        break;
//      case OBJECT_ARRAY:
//        arrayAccessor = new ObjectArrayAccessor( accessor );
//        break;
//      case PRIMITIVE_ARRAY:
//        arrayAccessor = new PrimitiveArrayAccessor( accessor, memberDef.type );
//        break;
//      default:
//        throw new IllegalStateException( "Undefined list type: " + listType );
//      }
//      DataDef memberDefn = memberDef.buildDef( (ObjectAccessor) arrayAccessor.memberAccessor() );
//      ListDef listDef = new ListDef( nullable, memberDefn );
//      listDef.arrayAccessor = arrayAccessor;
//      return listDef;
//    }
//
//    private DataDef buildMapDef( ObjectAccessor accessor) {
//    }

//    public void foo( ) {
//      if ( listType != null ) {
//        switch ( listType ) {
//        case ARRAY_OF_ARRAY:
//          break;
//        case LIST:
//          JavaListAccessor listAccessor = new JavaListAccessor( accessor );
//          ArrayFieldValue value = this.memberDef.buildArrayField( listAccessor, factory );
//          return FieldBuilder.buildTypedContainer(nullable, accessor, value );
//          break;
//        case OBJECT_ARRAY:
//          break;
//        case PRIMITIVE_ARRAY:
//          break;
//        case TYPED_OBJECT_ARRAY:
//          break;
//        default:
//          break;
//        
//        }
//        assert false;
//      } else if ( type.isVariant() ) {
//        VariantBoxedAccessor valueAccessor = new VariantBoxedAccessor( accessor, factory );
//        return new VariantFieldValueContainer( valueAccessor, factory );
//      } else {
//        AbstractFieldValue value = factory.buildValue( type );
//        value.bind( new BoxedAccessor( accessor ) );
//        return FieldBuilder.buildTypedContainer(nullable, accessor, value );
//      }
//      return null;
//    }

//    public FieldValueContainer buildContainer(FieldValueFactory factory, ArrayTupleHandle tupleHandle) {
//      ArrayFieldHandle accessor = new ArrayFieldHandle( tupleHandle, index );
//      return buildValue( factory, accessor );
//    }
//    
//    public FieldValueContainer buildValue(FieldValueFactory factory, ObjectAccessor accessor) {
//      if ( listType != null ) {
//        return buildList( factory, accessor );
//      }
//      if ( listType != null ) {
//        switch ( listType ) {
//        case ARRAY_OF_ARRAY:
//          break;
//        case LIST:
//          JavaListAccessor listAccessor = new JavaListAccessor( accessor );
//          ArrayFieldValue value = this.memberDef.buildArrayField( listAccessor, factory );
//          return FieldBuilder.buildTypedContainer(nullable, accessor, value );
//          break;
//        case OBJECT_ARRAY:
//          break;
//        case PRIMITIVE_ARRAY:
//          break;
//        case TYPED_OBJECT_ARRAY:
//          break;
//        default:
//          break;
//        
//        }
//        assert false;
//      } else if ( type.isVariant() ) {
//        VariantBoxedAccessor valueAccessor = new VariantBoxedAccessor( accessor, factory );
//        return new VariantFieldValueContainer( valueAccessor, factory );
//      } else {
//        AbstractFieldValue value = factory.buildValue( type );
//        value.bind( new BoxedAccessor( accessor ) );
//        return FieldBuilder.buildTypedContainer(nullable, accessor, value );
//      }
//      return null;
//    }
//
//    private ArrayFieldValue buildArrayField(ArrayAccessor arrayAccessor, FieldValueFactory factory) {
//      FieldValueContainer buildValue = buildValue( factory, )
//      ArrayValueImpl arrayValue;
//      if ( type.isVariant() ) {
//        arrayValue = new VariantArrayValue( arrayAccessor, type, factory );
//      } else {
//        arrayValue = new TypedArrayValue( arrayAccessor, type );
//      }
//      ArrayFieldValue value = FieldBuilder.buildArrayFieldValue( arrayAccessor, arrayValue );
//    }
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
      values[i] = fields[i].build( tupleHandle, factory);
    }
    return new FieldValueContainerSet( values );
  }
}
