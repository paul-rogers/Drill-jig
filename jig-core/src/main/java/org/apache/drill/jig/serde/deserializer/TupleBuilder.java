package org.apache.drill.jig.serde.deserializer;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.accessor.BoxedAccessor;
import org.apache.drill.jig.accessor.BoxedAccessor.VariantBoxedAccessor;
import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.Resetable;
import org.apache.drill.jig.accessor.JavaArrayAccessor;
import org.apache.drill.jig.accessor.JavaArrayAccessor.ObjectArrayAccessor;
import org.apache.drill.jig.accessor.JavaArrayAccessor.PrimitiveArrayAccessor;
import org.apache.drill.jig.accessor.JavaMapAccessor;
import org.apache.drill.jig.accessor.NullAccessor;
import org.apache.drill.jig.accessor.ReadOnceObjectAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.api.impl.DataDef;
import org.apache.drill.jig.api.impl.DataDef.ListDef;
import org.apache.drill.jig.api.impl.DataDef.MapDef;
import org.apache.drill.jig.api.impl.DataDef.ScalarDef;
import org.apache.drill.jig.container.FieldValueContainer;
import org.apache.drill.jig.container.FieldValueContainerSet;
import org.apache.drill.jig.serde.deserializer.BufferScalarAccessor.BufferScalarFieldAccessor;
import org.apache.drill.jig.serde.deserializer.BufferScalarAccessor.BufferVariantFieldAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.ArrayOfStructureAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.BooleanArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.DecimalArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Float32ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Float64ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Int16ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Int32ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Int64ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Int8ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.StringArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.VariantArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.BufferMapAccessor;
import org.apache.drill.jig.types.FieldValueFactory;

public class TupleBuilder {
  
  public abstract static class ValueNode {
    FieldSchema field;
    DataDef dataDef;
    Resetable resetable;
    
    public ValueNode(FieldSchema field) {
      this.field = field;
    }
    
    public abstract void bind( TupleSetDeserializer deserializer );
    public abstract void buildField( FieldValueFactory factory );
  }
  
  public static class NullNode extends ValueNode {

    FieldAccessor accessor = new NullAccessor( );
    
    public NullNode(FieldSchema field) {
      super(field);
    }

    @Override
    public void bind(TupleSetDeserializer deserializer) { }

    @Override
    public void buildField(FieldValueFactory factory) { }
  }
  
  public static class ScalarNode extends ValueNode {

    BufferScalarFieldAccessor accessor;
    
    public ScalarNode(FieldSchema field, BufferScalarFieldAccessor accessor) {
      super( field );
      this.accessor = accessor;
    }

    @Override
    public void bind( TupleSetDeserializer deserializer ) {
      accessor.bind( deserializer, field.index( ) );
    }

    @Override
    public void buildField(FieldValueFactory factory) {
      dataDef = new ScalarDef( field.type(), field.nullable(), accessor );
    }
  }
  
  public abstract static class StructureNode extends ValueNode {
    
    protected BufferStructureAccessor accessor;
    
    public StructureNode(FieldSchema field, BufferStructureAccessor accessor ) {
      super( field );
      this.accessor = accessor;
  }
  
  public abstract static class ListNode extends StructureNode {
    
    protected FieldSchema member;

    public ListNode(FieldSchema field, BufferStructureAccessor accessor ) {
      super( field, accessor );
      this.member = field.member( );
    }
   
    @Override
    public void bind(TupleSetDeserializer deserializer) {      
      accessor.bind( deserializer, field.index() );
    }
    
    protected void define( JavaArrayAccessor arrayAccessor, FieldAccessor memberAccessor ) {
      
      // Define the member and array data elements. The definitions will build the
      // field values and field value containers.
      
      DataDef memberDef = new ScalarDef( member.type(), member.nullable(), memberAccessor );
      dataDef = new ListDef( field.nullable(), memberDef, arrayAccessor );
    }
  }
  
  /**
   * Jig types that correspond to primitive Java types are deserialized into a
   * Java primitive array of the proper type. Then we use a Java primitive array
   * accessor to present the primitive Java array as a Jig array.
   * <p>
   * The array accessor builds a primitive Java array of the proper type
   * and presents it as a Java object.
   */
  
  public static class PrimitiveListNode extends ListNode {

    BufferStructureAccessor accessor;
    
    public PrimitiveListNode(FieldSchema field,
        BufferStructureAccessor accessor) {
      super( field, accessor );
    }

    @Override
    public void buildField(FieldValueFactory factory) {
      
      // The deserialized array is cached per-tuple, so add a reset to clear the
      // cached value.
      
      ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( accessor );
      resetable = cache;
      
      // Build a Java primitive array accessor to present our primitive array as
      // a Jig array.
      
      PrimitiveArrayAccessor arrayAccessor = new PrimitiveArrayAccessor( cache, member.type() );
      define( arrayAccessor, arrayAccessor.memberAccessor( ) );
    }

    @Override
    protected void buildNested() {
      // TODO Auto-generated method stub
      
    }
  }
  
  /**
   * Represents a serialized array as an array of Java objects. Used for Strings
   * and Decimals. The array is deserialized into a Java object array, which is
   * then presented as an object to a Java Object array accessor, which presents
   * the array to the client using the Jig Array API.
   * <p>
   * Create a variant array. A variant array is an array that holds any kind
   * of scalar value. Each value is written as a type/value pair. (Nulls are
   * written as the NULL type with no value.) This array is deserialized as a
   * Java object array what is then wrapped in a Java array accessor which
   * presents the variant array as a Jig array.
   */
  
  public static class ObjectListNode extends ListNode {

    public ObjectListNode(FieldSchema field,
        BufferStructureAccessor accessor) {
      super( field, accessor );
    }

    @Override
    public void buildField(FieldValueFactory factory) {
      // The provided array accessor deserializes the array into a Java object
      // array, and presents it as a Java object.
      
       // The array deserializer caches the deserialized Java array, so add it
      // as a per-tuple rest.
      
      ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( accessor );
      resetable = cache;
      
      // Create the accessor that presents the Java object array using the Jig
      // Array API.
      
      ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( cache );
      ObjectAccessor memberObjAccessor = (ObjectAccessor) objArrayAccessor.memberAccessor();
      
      BoxedAccessor memberAccessor;
      if ( member.type( ).isVariant( ) ) {
        
        // The member values are "boxed" Java objects. (Not really boxed for String
        // and decimal, but the idea also works for boxed Integers, etc.)
        // The important bit is that we expect all members to be of the
        // declared member type
        
        memberAccessor = new BoxedAccessor( memberObjAccessor );
      } else {
        
        // The member accessor is one that reads type/object pairs as
        // "boxed" Java objects. (That is, ints are stored as Integers, etc.)
        
        memberAccessor = new VariantBoxedAccessor( memberObjAccessor, factory );
      }
      
      define( objArrayAccessor, memberAccessor );
    }

    @Override
    protected void buildNested() {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  public static class StructureListNode extends ListNode {

    ValueNode element;
    
    public StructureListNode(FieldSchema field,
        StructureNode elementNode ) {
      super( field, new ArrayOfStructureAccessor( elementNode.accessor ) );
      element = elementNode;
    }
  }
  
  /**
   * Maps are deserialized into a Java map, then we use a Java map accessor to
   * present the Java map as a Jig map.
   */
  
  public static class MapNode extends StructureNode {

     public MapNode(FieldSchema field, BufferMapAccessor accessor) {
      super( field, accessor );
    }

    @Override
    public void bind(TupleSetDeserializer deserializer) {
      accessor.bind( deserializer, field.index() );
    }

    @Override
    public void buildField(FieldValueFactory factory) {
      // The buffer map accessor deserializes the map to a Java map,
      // which is presented as a Java object.
      
      ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( accessor );
      
      // The buffer map accessor caches the value for each tuple,
      // add a reset to clear the cached value on each new tuple.
      
      resetable = cache;
      
      // Use a Java map accessor to present the deserialized map to the
      // client via the Jig map API.
      
      dataDef = new MapDef( field.nullable(), new JavaMapAccessor( cache, factory ) );
    }

    private void buildNested() {
      // TODO Auto-generated method stub
      
    }

  }

  private TupleSetDeserializer deserializer;
  private FieldValueFactory factory;
  private List<Resetable> resets = new ArrayList<>( );

  public TupleBuilder( TupleSetDeserializer deserializer ) {
    this.deserializer = deserializer;
    factory = new FieldValueFactory( );
  }
  
  public AbstractTupleValue build( TupleSchema schema ) {
    int n = schema.count();
    ValueNode nodes[] = new ValueNode[n];
    for ( int i = 0; i < n;  i++ ) {
      nodes[i] = buildNode( schema.field( i ) );
      nodes[i].bind(deserializer);
      nodes[i].buildField(factory);
    }
    DataDef defs[] = new DataDef[n];
    for ( int i = 0; i < n;  i++ ) {
      ValueNode node = nodes[i];
      defs[i] = node.dataDef;
      if ( node.resetable != null )
        resets.add( node.resetable );
    }
    for ( int i = 0;  i < n;  i++ ) {
      defs[i].build( factory );
    }
    FieldValueContainer containers[] = new FieldValueContainer[n];
    for ( int i = 0;  i < n;  i++ ) {
      containers[i] = defs[i].container;
    }
    FieldValueContainerSet containerSet = new FieldValueContainerSet( containers );
    BufferTupleValue tuple = new BufferTupleValue( schema, containerSet );
    if ( ! resets.isEmpty() ) {
      tuple.resetable = new Resetable[resets.size()];
      resets.toArray( tuple.resetable );
    }
    return tuple;
  }
  
  private ValueNode buildNode(FieldSchema field) {
    switch ( field.type() ) {
    case BOOLEAN:
    case DECIMAL:
    case FLOAT32:
    case FLOAT64:
    case INT16:
    case INT32:
    case INT64:
    case INT8:
    case STRING:
      return new ScalarNode( field, new BufferScalarFieldAccessor( ) );
    case LIST:
      return buildListNode( field );
    case MAP:
      return new MapNode( field, new BufferMapAccessor( factory ) );
    case NULL:
    case UNDEFINED:
      return new NullNode( field );
    case NUMBER:
    case VARIANT:
      return new ScalarNode( field, new BufferVariantFieldAccessor( ) );
    case BLOB:
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case UTC_DATE_TIME:
    case TUPLE:
      throw new IllegalStateException( "Unsupported data type: " + field.type( ) );
    default:
      throw new IllegalStateException( "Unexpected data type: " + field.type( ) );
    
    }
  }

  private DataDef buildFieldDef(FieldSchema field) {
    switch ( field.type() ) {
    case BOOLEAN:
    case DECIMAL:
    case FLOAT32:
    case FLOAT64:
    case INT16:
    case INT32:
    case INT64:
    case INT8:
    case STRING:
      return buildScalar( field );
    case LIST:
      return buildList( field );
    case MAP:
      return buildMap( field );
    case NULL:
    case UNDEFINED:
      return buildNull( field );
    case NUMBER:
    case VARIANT:
      return buildVariant( field );
    case BLOB:
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case UTC_DATE_TIME:
    case TUPLE:
      throw new IllegalStateException( "Unsupported data type: " + field.type( ) );
    default:
      throw new IllegalStateException( "Unexpected data type: " + field.type( ) );
    
    }
  }

//  private ValueNode buildScalarNode(FieldSchema field) {
//    BufferScalarFieldAccessor accessor = ;
//  }

//  private DataDef buildScalar(FieldSchema field) {
//    BufferScalarFieldAccessor accessor = new BufferScalarFieldAccessor( );
//    accessor.bind( deserializer, field.index() );
//    return new ScalarDef( field.type(), field.nullable(), accessor );
//  }

//  private ValueNode buildNullNode(FieldSchema field) {
//  }

//  private DataDef buildNull(FieldSchema field) {
//    return new ScalarDef( field.type(), field.nullable(), new NullAccessor( ) );
//  }
//
//  private ValueNode buildVariantNode(FieldSchema field) {
//    return new ScalarNode( field, new BufferVariantFieldAccessor( ) );
//  }

//  private DataDef buildVariant(FieldSchema field) {
//    BufferVariantFieldAccessor accessor = new BufferVariantFieldAccessor( );
//    accessor.bind( deserializer, field.index() );
//    return new ScalarDef( field.type(), field.nullable(), accessor );
//  }
  
  private ListNode buildListNode(FieldSchema field) {
    FieldSchema member = field.member();
    BufferStructureAccessor accessor;
    switch ( member.type() ) {
    case BOOLEAN:
      return new PrimitiveListNode( field, new BooleanArrayAccessor( ) );
    case DECIMAL:
      return new ObjectListNode( field, new DecimalArrayAccessor( member.nullable() ) );
    case FLOAT32:
      return new PrimitiveListNode( field, new Float32ArrayAccessor( ) );
    case FLOAT64:
      return new PrimitiveListNode( field, new Float64ArrayAccessor( ) );
    case INT16:
      return new PrimitiveListNode( field, new Int16ArrayAccessor( ) );
    case INT32:
      return new PrimitiveListNode( field, new Int32ArrayAccessor( ) );
    case INT64:
      return new PrimitiveListNode( field, new Int64ArrayAccessor( ) );
    case INT8:
      return new PrimitiveListNode( field, new Int8ArrayAccessor( ) );
    case LIST:
      return new StructureListNode( field, buildListNode( field.member( ) ) );
    case MAP:
      return new StructureListNode( field, new MapNode( field.member( ), new BufferMapAccessor( factory ) ) );
    case STRING:
      return new ObjectListNode( field, new StringArrayAccessor( member.nullable() ) );
    case NUMBER:
    case VARIANT:
      return new ObjectListNode( field, new VariantArrayAccessor( factory ) );
    case BLOB:
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case NULL:
    case TUPLE:
    case UNDEFINED:
    case UTC_DATE_TIME:
      throw new IllegalStateException( "Unsupported array type: " + member.type() );
    default:
      throw new IllegalStateException( "Unexpected array type: " + member.type( ) );   
    }
  }


//  private DataDef buildList(FieldSchema field) {
//    FieldSchema member = field.member();
//    switch ( member.type() ) {
//    case BOOLEAN:
//      return primitiveArray( new BooleanArrayAccessor( ), field );
//    case DECIMAL:
//      return typedObjectArray( new DecimalArrayAccessor( member.nullable() ), field );
//    case FLOAT32:
//      return primitiveArray( new Float32ArrayAccessor( ), field );
//    case FLOAT64:
//      return primitiveArray( new Float64ArrayAccessor( ), field );
//    case INT16:
//      return primitiveArray( new Int16ArrayAccessor( ), field );
//    case INT32:
//      return primitiveArray( new Int32ArrayAccessor( ), field );
//    case INT64:
//      return primitiveArray( new Int64ArrayAccessor( ), field );
//    case INT8:
//      return primitiveArray( new Int8ArrayAccessor( ), field );
//    case LIST:
//    case MAP:
//      return structureArray( field );
//    case STRING:
//      return typedObjectArray( new StringArrayAccessor( member.nullable() ), field );
//    case NUMBER:
//    case VARIANT:
//      return variantArray( field );
//    case BLOB:
//    case DATE:
//    case DATE_TIME_SPAN:
//    case LOCAL_DATE_TIME:
//    case NULL:
//    case TUPLE:
//    case UNDEFINED:
//    case UTC_DATE_TIME:
//      throw new IllegalStateException( "Unsupported array type: " + member.type() );
//    default:
//      throw new IllegalStateException( "Unexpected array type: " + member.type( ) );   
//    }
//  }

//  /**
//   * Jig types that correspond to primitive Java types are deserialized into a
//   * Java primitive array of the proper type. Then we use a Java primitive array
//   * accessor to present the primitive Java array as a Jig array.
//   * 
//   * @param arrayAccessor array deserializer of the proper type
//   * @param field
//   * @return
//   */
  
//  private ListNode primitiveArrayNode( BufferStructureAccessor arrayAccessor, FieldSchema field ) {    
//    ValueNode memberNode = new ScalarNode( field.member(), null );
//    return new PrimitiveListNode( field, arrayAccessor );
//  }

//  private ListDef primitiveArray( BufferStructureAccessor arrayAccessor, FieldSchema field ) {
//    
//    // The array accessor builds a primitive Java array of the proper type
//    // and presents it as a Java object.
//    
//    arrayAccessor.bind( deserializer, field.index() );
//    
//    
//    // Build a Java primitive array accessor to present our primitive array as
//    // a Jig array.
//    
//    FieldSchema member = field.member();
//    PrimitiveArrayAccessor accessor = new PrimitiveArrayAccessor( cache, member.type() );
//    
//    // Define the member and array data elements. The definitions will build the
//    // field values and field value containers.
//    
//    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), accessor.memberAccessor( ) );
//    return new ListDef( field.nullable(), memberDef, accessor );
//  }

//  /**
//   * Represents a serialized array as an array of Java objects. Used for Strings
//   * and Decimals. The array is deserialized into a Java object array, which is
//   * then presented as an object to a Java Object array accessor, which presents
//   * the array to the client using the Jig Array API.
//   * 
//   * @param arrayAccessor accessor that deserializes teh array to the proper
//   * type
//   * @param field
//   * @return
//   */
//  
//  private ListDef typedObjectArray(BufferStructureAccessor arrayAccessor,
//      FieldSchema field) {
//    
//    // The provided array accessor deserializes the array into a Java object
//    // array, and presents it as a Java object.
//    
//    arrayAccessor.bind( deserializer, field.index() );
//    
//    // The array deserializer caches the deserialized Java array, so add it
//    // as a per-tuple rest.
//    
//    ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( arrayAccessor );
//    resets.add( cache );
//    
//    // Create the accessor that presents the Java object array using the Jig
//    // Array API.
//    
//    ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( cache );
//    
//    // The member values are "boxed" Java objects. (Not really boxed for String
//    // and decimal, but the idea also works for boxed Integers, etc.)
//    // The important bit is that we expect all members to be of the
//    // declared member type
//    
//    BoxedAccessor memberAccessor = new BoxedAccessor( (ObjectAccessor) objArrayAccessor.memberAccessor() );
//    
//    // Build the data element definitions that will build the array and member
//    // field values and field value containers.
//    
//    FieldSchema member = field.member();
//    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), memberAccessor );
//    return new ListDef( field.nullable(), memberDef, objArrayAccessor );
//  }

//  /**
//   * Create a variant array. A variant array is an array that holds any kind
//   * of scalar value. Each value is written as a type/value pair. (Nulls are
//   * written as the NULL type with no value.) This array is deserialized as a
//   * Java object array what is then wrapped in a Java array accessor which
//   * presents the variant array as a Jig array.
//   * 
//   * @param field
//   * @return
//   */
//  
//  private ListDef variantArray(FieldSchema field) {
//    
//    // Create an accessor to deserialize a variant array. Since variants
//    // carry their own type information, no type information is require
//    // for array members.
//    
//    FieldSchema member = field.member();
//    VariantArrayAccessor arrayAccessor = new VariantArrayAccessor( factory );
//    arrayAccessor.bind( deserializer, field.index() );   
//    
//    // The constructed object array is cached, so add a reset to clear
//    // the cache on each tuple.
//    
//    ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( arrayAccessor );
//    resets.add( cache );
//    
//    // The variant array accessor is presented as an object accessor to
//    // a Java object array accessor.
//    
//    ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( cache );
//    
//    // The member accessor is one that reads type/object pairs as
//    // "boxed" Java objects. (That is, ints are stored as Integers, etc.)
//    
//    BoxedAccessor memberAccessor = new VariantBoxedAccessor( (ObjectAccessor) objArrayAccessor.memberAccessor(), factory );
//    
//    // Create the data element definition that will create the FieldValue API
//    // objects and the corresponding variant container to provide the correct
//    // FieldValue object for each data item type.
//    
//    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), memberAccessor );
//    return new ListDef( field.nullable(), memberDef, objArrayAccessor );
//  }

//  private ListDef structureArray2(FieldSchema field) {
//    DataDef memberDef;
//    FieldSchema member = field.member();
//    if ( member.type( ) == DataType.MAP ) {
//      memberDef = buildMap( member );
//    } else if ( member.type( ) == DataType.LIST ) {
//      memberDef = buildList( member );
//    } else {
//      throw new IllegalStateException( "Not a structure type: " + member.type() );
//    }
//    ArrayOfStructureAccessor topAccessor = new ArrayOfStructureAccessor( memberDef.structureAccessor );
//    topAccessor.bind( deserializer, field.index() );
//    ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( topAccessor );
//    resets.add( cache );
//    ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( cache );
//    return new ListDef( field.nullable(), memberDef.dataDef, objArrayAccessor );
//  }

  private ListDef structureArray(FieldSchema field) {
    FieldSchema member = field.member();
    BufferStructureAccessor innerAccessor;
    if ( member.type() == DataType.LIST ) {
      FieldSchema innerMember = member.member();
      switch ( innerMember.type() ) {
      case BOOLEAN:
        innerAccessor = new BooleanArrayAccessor( );
        break;
      case DECIMAL:
        innerAccessor = new DecimalArrayAccessor( innerMember.nullable() );
        break;
      case FLOAT32:
        innerAccessor = new Float32ArrayAccessor( );
        break;
      case FLOAT64:
        innerAccessor = new Float64ArrayAccessor( );
        break;
      case INT16:
        innerAccessor = new Int16ArrayAccessor( );
        break;
      case INT32:
        innerAccessor = new Int32ArrayAccessor( );
        break;
      case INT64:
        innerAccessor = new Int64ArrayAccessor( );
        break;
      case INT8:
        innerAccessor = new Int8ArrayAccessor( );
        break;
      case LIST:
        throw new IllegalStateException( "Lists can nest to only 1 level" );
      case MAP:
        innerAccessor = new ArrayOfStructureAccessor( new BufferMapAccessor( factory ) );
        break;
      case STRING:
        innerAccessor = new StringArrayAccessor( innerMember.nullable() );
        break;
      case NUMBER:
      case VARIANT:
        innerAccessor = new VariantArrayAccessor( factory );
        break;
      case BLOB:
      case DATE:
      case DATE_TIME_SPAN:
      case LOCAL_DATE_TIME:
      case NULL:
      case TUPLE:
      case UNDEFINED:
      case UTC_DATE_TIME:
        throw new IllegalStateException( "Unsupported array type: " + innerMember.type() );
      default:
        throw new IllegalStateException( "Unexpected array type: " + innerMember.type( ) );   
      }
    } else if ( member.type( ) == DataType.MAP ) {
      innerAccessor = new BufferMapAccessor( factory );
    } else {
      throw new IllegalStateException( "Not a structure type: " + member.type() );
    }
    ArrayOfStructureAccessor topAccessor = new ArrayOfStructureAccessor( innerAccessor );
    topAccessor.bind( deserializer, field.index() );
    ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( topAccessor );
    resets.add( cache );
    ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( cache );
    DataDef memberDef;
    if ( member.type( ) == DataType.MAP ) {
      memberDef = buildNestedMap( member, (ObjectAccessor) objArrayAccessor.memberAccessor() );
    } else if ( member.type( ) == DataType.LIST ) {
      memberDef = buildNestedList( member, (ObjectAccessor) objArrayAccessor.memberAccessor() );
    } else {
      throw new IllegalStateException( "Not a structure type: " + member.type() );
    }
    return new ListDef( field.nullable(), memberDef, objArrayAccessor );
  }

  private MapDef buildNestedMap(FieldSchema member,
      ObjectAccessor memberAccessor) {
    return new MapDef( member.nullable(), new JavaMapAccessor( memberAccessor, factory ) );
  }

  private ListDef buildNestedList(FieldSchema listSchema,
      ObjectAccessor listAccessor) {
    FieldSchema member = listSchema.member( );
    FieldAccessor memberAccessor;
    JavaArrayAccessor arrayAccessor;
    if ( member.type( ).isVariant() ) {
      arrayAccessor = new ObjectArrayAccessor( listAccessor );
      memberAccessor = new VariantBoxedAccessor( (ObjectAccessor) arrayAccessor.memberAccessor(), factory );
    } else if ( member.type( ).isPrimitive( ) ) {
      arrayAccessor = new PrimitiveArrayAccessor( listAccessor, member.type() );
      memberAccessor = arrayAccessor.memberAccessor( );
    } else {
      arrayAccessor = new ObjectArrayAccessor( listAccessor );
      memberAccessor = new BoxedAccessor( (ObjectAccessor) arrayAccessor.memberAccessor() );
    }
    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), memberAccessor );
    return new ListDef( listSchema.nullable(), memberDef, arrayAccessor );
  }

  /**
   * Maps are deserialized into a Java map, then we use a Java map accessor to
   * present the Java map as a Jig map.
   * 
   * @param field
   * @return
   */
  
//  private ValueNode buildMapNode(FieldSchema field) {
//    
//    // The buffer map accessor deserializes the map to a Java map,
//    // which is presented as a Java object.
//    
//    BufferMapAccessor accessor = new BufferMapAccessor( factory );
//    return new MapNode( field, new BufferMapAccessor( factory ) );
//  }

//  private MapDef buildMap(FieldSchema field) {
//    
//    // The buffer map accessor deserializes the map to a Java map,
//    // which is presented as a Java object.
//    
//    BufferMapAccessor accessor = new BufferMapAccessor( factory );
//    accessor.bind( deserializer, field.index() );
//    ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( accessor );
//    
//    // The buffer map accessor caches the value for each tuple,
//    // add a reset to clear the cached value on each new tuple.
//    
//    resets.add( cache );
//    
//    // Use a Java map accessor to present the deserialized map to the
//    // client via the Jig map API.
//    
//    return new MapDef( field.nullable(), new JavaMapAccessor( cache, factory ) );
//  }

}
