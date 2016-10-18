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
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.BufferMapAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.DecimalArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Float32ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Float64ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Int16ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Int32ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Int64ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.Int8ArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.StringArrayAccessor;
import org.apache.drill.jig.serde.deserializer.BufferStructureAccessor.VariantArrayAccessor;
import org.apache.drill.jig.types.FieldValueFactory;

/**
 * Build the tuple value that deserializes fields on demand, materializing
 * lists and maps, and caching values where needed. The tuple value is built
 * up over several steps:
 * <ol>
 * <li>Start with the schema of each field.</li>
 * <li>Create an intermediate node of the proper type. A wide range of
 * strategies are used to deserialize data; the intermediate nodes capture
 * that logic.</li>
 * <li>Use the intermediate nodes and/or field types to build up the
 * deserialization accessors that read data from an input buffer.</li>
 * <li>For materialized arrays and maps, Java object accessors that
 * present data as Jig types.</li>
 * <li>{@link DataDef} nodes that define the required field values and
 * containers.</li>
 * <li>Finally, the assembled tuple value.</li>
 * </ul>
 */

public class TupleBuilder {
  
  /**
   * The value node represents either a field (top level within a tuple)
   * or a nested item (in a list). The subclasses abstract out the specialized
   * logic needed to construct each field type.
   */
  
  public abstract static class ValueNode {
    FieldSchema field;
    DataDef dataDef;
    Resetable resetable;
    
    public ValueNode(FieldSchema field) {
      this.field = field;
    }
    
    /**
     * Bind top-level fields to the serializer and field index.
     * @param deserializer
     */
    
    public abstract void bind( TupleSetDeserializer deserializer );
    
    /**
     * Construct the accessors needed to present the field value.
     * 
     * @param factory
     */
    
    public abstract void buildField( FieldValueFactory factory );
  }
  
  /**
   * Represents a field of type NULL or UNDEFINED.
   */
  
  public static class NullNode extends ValueNode {

    FieldAccessor accessor = new NullAccessor( );
    
    public NullNode(FieldSchema field) {
      super(field);
    }

    @Override
    public void bind(TupleSetDeserializer deserializer) { }

    @Override
    public void buildField(FieldValueFactory factory) {
      dataDef = new ScalarDef( field.type(), field.nullable(), accessor );
    }
  }
  
  /**
  * Jig types that correspond to primitive Java types are deserialized into a
  * Java primitive array of the proper type. Then we use a Java primitive array
  * accessor to present the primitive Java array as a Jig array.
  */
  
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
  
  /**
   * Represents a Jig type deserialized into a Java structure such as an array
   * or Map. A deserializer accessor materializes the value as a Java
   * structure. A caching accessor caches the value per tuple to avoid
   * excess deserializations. A Java object accessor then presents the
   * object as the proper Jig structure type.
   */
  
  public abstract static class StructureNode extends ValueNode {
    
    /**
     * The accessor used to deserialize the structure.
     */
    
    protected BufferStructureAccessor accessor;
    
    /**
     * The accessor used to fetch the structure as a Java object.
     * This accessor inserts a caching layer above the deserializer.
     * The caching layer is used only for top-level structures, but
     * not for structures nested inside of a list.
     */
    
    protected ObjectAccessor source;
    
    public StructureNode(FieldSchema field, BufferStructureAccessor accessor ) {
      super( field );
      this.accessor = accessor;
      
      // The buffer structure accessor deserializes the structure to a Java Map,
      // or array which is presented as a Java object.
      
      ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( accessor );
      
      // The read once accessor caches the value for each tuple,
      // add a reset to clear the cached value on each new tuple.
      
      resetable = cache;
      this.source = cache;
   }
    
    @Override
    public void bind(TupleSetDeserializer deserializer) {      
      accessor.bind( deserializer, field.index() );
    }

    @Override
    public void buildField(FieldValueFactory factory) {
      buildJavaAccessor( source, factory );
    }
    
    /**
     * Builds the accessor to present a Java map or array. The source
     * object accessor is the cached object accessor (for top-level fields)
     * or the member accessor for a parent list.
     * 
     * @param objAccessor
     * @param factory
     */
    
    protected abstract void buildJavaAccessor( ObjectAccessor objAccessor, 
                                               FieldValueFactory factory );
  }
  
  /**
   * Represents the various Java array types used to hold deserialized
   * lists.
   */
  
  public abstract static class ListNode extends StructureNode {
    
    protected FieldSchema member;

    public ListNode(FieldSchema field, BufferStructureAccessor accessor ) {
      super( field, accessor );
      this.member = field.member( );
    }
    
    /**
     * Define the field definition for a list and its elements. Used for the
     * simple case in which the list contains scalar elements (primitives
     * or simple objects.)
     * 
     * @param arrayAccessor
     * @param elementAccessor
     */
    
    protected void define( JavaArrayAccessor arrayAccessor, FieldAccessor elementAccessor ) {
      
      // Define the member and array data elements. The definitions will build the
      // field values and field value containers.
      
      DataDef memberDef = new ScalarDef( member.type(), member.nullable(), elementAccessor );
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
    protected void buildJavaAccessor( ObjectAccessor objAccessor, FieldValueFactory factory ) {
      
      // Build a Java primitive array accessor to present our primitive array as
      // a Jig array.
      
      PrimitiveArrayAccessor arrayAccessor = new PrimitiveArrayAccessor( objAccessor, member.type() );
      define( arrayAccessor, arrayAccessor.memberAccessor( ) );
    }
  }
  
  /**
   * Represents a serialized array as an array of Java objects. Used for Strings
   * and Decimals. The array is deserialized into a Java object array, which is
   * then presented as an object to a Java Object array accessor, which presents
   * the array to the client using the Jig Array API.
   * <p>
   * Also represents a variant array. A variant array is an array that holds any kind
   * of scalar value. Each value is written as a type/value pair. (Nulls are
   * written as the NULL type with no value.) This array is deserialized as a
   * Java object array what is then wrapped in a Java array accessor which
   * presents the variant array as a Jig array. Since the type of elements
   * varies, elements are represented in boxed (object) form.
   */
  
  public static class ObjectListNode extends ListNode {

    public ObjectListNode(FieldSchema field,
        BufferStructureAccessor accessor) {
      super( field, accessor );
    }

    @Override
    protected void buildJavaAccessor( ObjectAccessor objAccessor, FieldValueFactory factory ) {
      
      // Create the accessor that presents the Java object array using the Jig
      // Array API.
      
      ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( objAccessor );
      ObjectAccessor memberObjAccessor = (ObjectAccessor) objArrayAccessor.memberAccessor();
      
      BoxedAccessor memberAccessor;
      if ( member.type( ).isVariant( ) ) {
        
        // The member accessor is one that reads type/object pairs as
        // "boxed" Java objects. (That is, ints are stored as Integers, etc.)
        
        memberAccessor = new VariantBoxedAccessor( memberObjAccessor, factory );
      } else {
        
        // The member values are "boxed" Java objects. (Not really boxed for String
        // and decimal, but the idea also works for boxed Integers, etc.)
        // The important bit is that we expect all members to be of the
        // declared member type
        
        memberAccessor = new BoxedAccessor( memberObjAccessor );
      }
      
      define( objArrayAccessor, memberAccessor );
    }   
  }
  
  /**
   * Represents an array of structures (list of list or list of Map).
   * The structures are deserialized using the proper deserializer for
   * the type of structure. Then, the deserialized structures are
   * assembled into a Java object array, which is then presented as a Jig
   * array.
   */
  
  public static class StructureListNode extends ListNode {

    StructureNode element;
    
    public StructureListNode(FieldSchema field,
        StructureNode elementNode ) {
      super( field, new ArrayOfStructureAccessor( elementNode.accessor,
                                            field.member( ).nullable() ) );
      element = elementNode;
    }

    @Override
    protected void buildJavaAccessor(ObjectAccessor objAccessor, FieldValueFactory factory) {
      if ( source == null )
        throw new IllegalStateException( "Lists can nest to only 1 level" );
      ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( objAccessor );
      ObjectAccessor memberAccessor = (ObjectAccessor) objArrayAccessor.memberAccessor();
      element.buildJavaAccessor( memberAccessor, factory );
      dataDef = new ListDef( field.nullable(), element.dataDef, objArrayAccessor );
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
    protected void buildJavaAccessor( ObjectAccessor objAccessor, FieldValueFactory factory ) {
      
      // Use a Java map accessor to present the deserialized map to the
      // client via the Jig map API.
      
      dataDef = new MapDef( field.nullable(), new JavaMapAccessor( objAccessor, factory ) );
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
    FieldValueContainer containers[] = new FieldValueContainer[n];
    for ( int i = 0; i < n;  i++ ) {
      containers[i] = buildContainer( schema.field( i ) );
    }
    FieldValueContainerSet containerSet = new FieldValueContainerSet( containers );
    BufferTupleValue tuple = new BufferTupleValue( schema, containerSet );
    if ( ! resets.isEmpty() ) {
      tuple.resetable = new Resetable[resets.size()];
      resets.toArray( tuple.resetable );
    }
    return tuple;
  }
  
  /**
   * Build the container of field values for the field. Builds an intermediate
   * node based on the field type, then uses that node to build the
   * deserializer accessors and (for structured types), the intermediate
   * Java representations.
   * 
   * @param field
   * @return
   */
  
  private FieldValueContainer buildContainer(FieldSchema field) {
    ValueNode node = buildNode( field );
    node.bind(deserializer);
    node.buildField(factory);
    if ( node.resetable != null )
      resets.add( node.resetable );
    node.dataDef.build( factory );
    return node.dataDef.container;
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

  /**
   * Build a list node that depends on the type of member. Primitive members
   * are deserialized into primitive Java arrays. Strings and decimals are
   * deserialized into object arrays. Structured types are deserialized
   * using their own deserializers, then held in an object array.
   * 
   * @param field
   * @return
   */
  
  private ListNode buildListNode(FieldSchema field) {
    FieldSchema member = field.member();
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
      return new StructureListNode( field, buildListNode( member ) );
    case MAP:
      return new StructureListNode( field, new MapNode( member, new BufferMapAccessor( factory ) ) );
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
}
