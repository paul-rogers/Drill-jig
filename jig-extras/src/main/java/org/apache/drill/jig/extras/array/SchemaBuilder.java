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
import org.apache.drill.jig.types.DataDef.*;
import org.apache.drill.jig.types.FieldAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldValueContainer;
import org.apache.drill.jig.types.FieldValueContainerSet;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.JavaArrayAccessor.ObjectArrayAccessor;
import org.apache.drill.jig.types.JavaArrayAccessor.PrimitiveArrayAccessor;
import org.apache.drill.jig.types.JavaListAccessor;
import org.apache.drill.jig.types.JavaMapAccessor;
import org.apache.drill.jig.types.NullAccessor;

/**
 * Create the schema and field value/accessor tree for a result
 * set backed by a Java-array-based data set. The work is done in
 * three passes:
 * <ol>
 * <li>Scan the data arrays to determine field types.</li>
 * <li>Build up the accessor tree based on the types.</li>
 * <li>Build up the data value tree based on the accessors
 * and data types.</li>
 * </ol>
 */

public class SchemaBuilder {
  public static class FieldDefn {
    public enum ListType {
      LIST, OBJECT_ARRAY, TYPED_OBJECT_ARRAY, PRIMITIVE_ARRAY
    }

    private final int index;
    private boolean nullable = false;
    private DataType type = DataType.UNDEFINED;
    private ListType listType;
    private FieldDefn memberDef;
    private FieldAccessor accessor;

    public FieldDefn(int index) {
      this.index = index;
    }

    /**
     * Determine the data type of each column. For columns that
     * are either a java {@link List} or array, recursively determine
     * the types of the lists. Lists and arrays can nest to any level.
     * Scalar types can mix (strings and numbers, say), but lists
     * and maps cannot mix. Maps must have string keys (implicit)
     * and can have values of any scalar type.
     * 
     * @param object
     * @param factory
     */
    
    @SuppressWarnings("unchecked")
    public void define(Object object, FieldValueFactory factory) {
      nullable |= (object == null);
      if (!buildArray(object, factory)) {
        type = factory.mergeTypes(type, factory.objectToJigType(object));
        if (type == DataType.LIST  &&  object != null ) {
          listType = ListType.LIST;
          buildList( (List<Object>) object, factory );
        }
      }
    }

    /**
     * Determine the type of an array. Supports object arrays:<br>
     * <code>String []</code><br>
     * say, or scalar arrays:<br>
     * <code>int[]</code><br>
     * or even generic Object arrays:<br>
     * <code>Object []</code><br>
     * which can contain (boxed) scalars or another array
     * or list.
     * 
     * @param object
     * @param factory
     * @return
     */
    private boolean buildArray(Object object, FieldValueFactory factory) {
      if (object == null)
        return false;
      String objClass = object.getClass().getName();
      if (!objClass.startsWith("["))
        return false;
      type = factory.mergeTypes(type, DataType.LIST);

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
       int posn = objClass.indexOf(';');
        
        // If the array is of type Object, then we have no type info.
        
        String memberClassName = objClass.substring( 2, posn );
        if ( memberClassName.equals( Object.class.getName() ) ) {
          thisType = ListType.OBJECT_ARRAY;
          memberType = DataType.UNDEFINED;
        } else {
          thisType = ListType.TYPED_OBJECT_ARRAY;
          memberType = factory.classNameToJigType( memberClassName );
        }
        break;
      default:
        throw new ValueConversionError( "Unknown Java array type: " + second );
      }

      if (listType == null)
        listType = thisType;
      else if (listType != thisType)
        throw new ValueConversionError("Incompatible arrays: " + listType
            + " and " + thisType);

      if ( memberDef == null ) {
        memberDef = new FieldDefn( 0 );
        memberDef.type = memberType;
      }
      switch ( listType ) {
      case OBJECT_ARRAY: {
        
        // Infer the type and nullability by scanning list members.
        
        int n = Array.getLength( object );
        for ( int i = 0;  i < n;  i++ ) {
          memberDef.define(Array.get(object, i), factory);
        }
        break;
      }
        
      case TYPED_OBJECT_ARRAY: {
        
        if ( memberDef.type != memberType) {
          throw new ValueConversionError( "Cannot mix object array types: " + memberDef.type + " and " + memberType );
        }
        
        // Infer the nullability by scanning list members. Even if
        // we are given the type, we still need to check members to determine
        // nullability.
        
        int n = Array.getLength( object );
        for ( int i = 0;  i < n;  i++ ) {
          memberDef.define(Array.get(object, i), factory);
        }
        break;
      }
        
      case PRIMITIVE_ARRAY:
        if ( memberDef.type != memberType ) {
          throw new ValueConversionError( "Cannot mix primitive array types: " + memberDef.type + " and " + memberType );
        }
        break;
      default:
        throw new IllegalStateException( "Unexpected list type: " + listType );
      }
      return true;
    }
    
    /**
     * Infer list member type from actual members. (The parameterized type, if any,
     * is erased in the data, so cannot be used here.)
     * 
     * @param list
     * @param factory
     */
    private void buildList(List<Object> list, FieldValueFactory factory) {
      if ( memberDef == null )
        memberDef = new FieldDefn( 0 );
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
    
    /**
     * Build the accessor tree top-down.
     * 
     * @param tupleHandle
     * @param factory
     */

    public void buildTupleFieldAccessor( ArrayTupleHandle tupleHandle, FieldValueFactory factory ) {
      ArrayFieldHandle fieldHandle = new ArrayFieldHandle( tupleHandle, index );
      buildFieldAccessor( fieldHandle, factory );
    }
    
    public void buildFieldAccessor( ObjectAccessor objAccessor, FieldValueFactory factory ) {
      if ( listType != null ) {
        buildListAccessor( objAccessor, factory );
      } else if ( type == DataType.MAP ) {
        accessor = new JavaMapAccessor( objAccessor, factory );
      } else if ( type.isUndefined( ) ) {
        accessor = new NullAccessor( );
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
        memberDef.buildFieldAccessor( (ObjectAccessor) arrayAccessor.memberAccessor(), factory );
        break;
      case OBJECT_ARRAY:
      case TYPED_OBJECT_ARRAY:
        arrayAccessor = new ObjectArrayAccessor( objAccessor );
        memberDef.buildFieldAccessor( (ObjectAccessor) arrayAccessor.memberAccessor(), factory );
        break;
      case PRIMITIVE_ARRAY:
        arrayAccessor = new PrimitiveArrayAccessor( objAccessor, memberDef.type );
        memberDef.accessor = arrayAccessor.memberAccessor();
        break;
      default:
        throw new IllegalStateException( "Undefined list type: " + listType );
      }
      accessor = arrayAccessor;
    }

    /**
     * Build the definitions used to generate the field value tree.
     * 
     * @return
     */
    
    public DataDef buildDef( ) {
      if ( listType != null ) {
        return new ListDef( nullable, memberDef.buildDef( ), (ArrayAccessor) accessor );
      } else if ( type == DataType.MAP ) {
        return new MapDef( nullable, (MapValueAccessor) accessor );
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
  }
  
  FieldValueFactory factory;
  
  private FieldDefn fields[];
  public SchemaBuilder(FieldValueFactory factory ) {
    this.factory = factory;
  }

  public TupleSchema buildSchema(Batch batch ) {
    int fieldCount = batch.names.length;
    fields = new FieldDefn[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      fields[i] = new FieldDefn(i);
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
