package org.apache.drill.jig.direct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.Text;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.jig.accessor.BoxedAccessor;
import org.apache.drill.jig.accessor.BoxedAccessor.VariantBoxedAccessor;
import org.apache.drill.jig.accessor.CachedObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.Resetable;
import org.apache.drill.jig.accessor.JavaListAccessor;
import org.apache.drill.jig.accessor.ReadOnceObjectAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.impl.ArrayFieldSchemaImpl;
import org.apache.drill.jig.api.impl.DataDef;
import org.apache.drill.jig.api.impl.DataDef.ListDef;
import org.apache.drill.jig.api.impl.DataDef.ScalarDef;
import org.apache.drill.jig.api.impl.DataDef.TupleDef;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.container.FieldValueContainer;
import org.apache.drill.jig.container.FieldValueContainerSet;
import org.apache.drill.jig.direct.DirectTupleValue.DrillRootTupleValue;
import org.apache.drill.jig.direct.DrillTypeConversion.DrillDataType;
import org.apache.drill.jig.direct.MapVectorAccessor.RepeatedMapVectorAccessor;
import org.apache.drill.jig.direct.VectorAccessor.DrillElementAccessor;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.ArrayFieldValue.JavaListFieldValue;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.MapFieldValue.JavaMapFieldValue;

/**
 * Build a Jig schema and tuple implementation given a Drill schema.
 * Maps Jig field values to Drill vector accessors. Creates "virtual"
 * fields to represent Drill repeated types (presented as an array)
 * and Drill maps (presented as a tuple.)
 */

public class TupleBuilder
{
  public static final Object DATA_FIELD_NAME = "$data$";
  public static final Object OFFSETS_FIELD_NAME = "$offsets$";
  
  /**
   * Specialized "boxed" accessor that expects {@link Text} objects
   * instead of Java String objects for string values.
   */
  
  public static class DrillBoxedAccessor extends BoxedAccessor
  {
    public DrillBoxedAccessor(ObjectAccessor accessor) {
      super(accessor);
    }

    @Override
    public String getString() {
      return ((Text) getObject( )).toString();
    }    
  }
  
  /**
   * Specialized variant "boxed" accessor that expects {@link Text} objects
   * instead of Java String objects for string values.
   */
  
  public static class DrillVariantBoxedAccessor extends VariantBoxedAccessor
  {
    public DrillVariantBoxedAccessor(ObjectAccessor accessor,
        FieldValueFactory factory) {
      super(accessor, factory);
    }

    @Override
    public String getString() {
      return ((Text) getObject( )).toString();
    }    
  }
  
  /**
   * Specialized field value factory that handles the special cases for
   * Drill.
   */
  
  public static class DrillFieldValueFactory extends FieldValueFactory
  {
    @Override
    protected AbstractFieldValue extendedValue(DataType type) {
      if ( type == DataType.LIST ) {
        return new JavaListFieldValue( this );
      }
      if ( type == DataType.MAP ) {
        return new JavaMapFieldValue( this );
      }
      return super.extendedValue(type);
    }
    
    @Override
    protected DataType extendedConversion(Class<? extends Object> valueClass) {
      if ( valueClass.isAssignableFrom( Text.class ) )
        return DataType.STRING;
      return super.extendedConversion( valueClass );
    }
  
    @Override
    public FieldAccessor newVariantObjectAccessor( ObjectAccessor objAccessor ) {
      return new DrillVariantBoxedAccessor( objAccessor, this );
    }
  }
  
  /**
   * In Drill, records and maps are both collections of vectors. Both
   * map to a Jig tuple.
   */
  
  public static abstract class TupleNode {
    
    protected final TupleSchemaImpl schema = new TupleSchemaImpl( );
    protected final List<FieldNode> nodes = new ArrayList<>( );
    FieldValueContainerSet containerSet;
    
    /**
     * Add a field by "parsing" the Drill matrialized field into an
     * internal {@link FieldNode} that will build the Jig mechanism
     * required to access the vector for the field.
     * 
     * @param batchField
     * @param vectorIndex
     */
    
    protected void addField( MaterializedField batchField, int vectorIndex ) {
      FieldNode node = makeField( batchField );
      node.vectorIndex = vectorIndex;
      node.buildSchema( );
      schema.add( node.schema );
      nodes.add( node );
    }
    
    /**
     * Build the node for a field depending on type and data mode.
     * 
     * @param batchField
     * @return
     */
    
    private FieldNode makeField(MaterializedField batchField) {
      
      // Repeated fields are presented in Jig as an array.
      
      MinorType drillType = batchField.getType().getMinorType();
      if ( drillType == MinorType.LIST ) {
        return buildListNode( batchField );
      } else if ( batchField.getDataMode() == DataMode.REPEATED ) {        
        if ( drillType == MinorType.MAP ) {
          
          // Repeated maps require special treatment. 
          
          return new RepeatedMapNode( batchField );
        } else {          
          
          // All other (simple) types work in a common way.
          // Per-type differences are factored out to an
          // element associated with the repeated node.
          
          return new RepeatedNode( batchField, buildNode( batchField, drillType ) );
        }
      } else {
        
        // Build a scalar node
        
        return buildNode( batchField, drillType );
      }
    }
   
    /**
     * Build an internal node for non-repeated types.
     * 
     * @param batchField
     * @param drillType
     * @return
     */
    
    private FieldNode buildNode( MaterializedField batchField, MinorType drillType ) {
      if ( drillType == MinorType.MAP ) {
        return new MapNode( batchField );
      } else {
        return new ScalarNode( batchField );
      }
    }

    /**
     * List nodes require special handling. A list can be a simple list
     * (with one child), which Jig treats as an array of that type. Or
     * the list can be a repeat of an (implied) map which Jig presents
     * as an array of maps.
     * 
     * @param batchField
     * @return
     */
    
    private FieldNode buildListNode(MaterializedField batchField) {
      Collection<MaterializedField> children = batchField.getChildren();
      
      // The nested field named "$data$" carries the type information
      // for elements. The children are stored in a map, but it seems
      // that the map is not exposed, so we have to iterate over the
      // fields to find "$data$".
      
      FieldNode child = null;
      for ( MaterializedField childField : children ) {
        if ( childField.getName().equals( DATA_FIELD_NAME ) ) {
          child = makeField( childField );
        }
      }
      assert child != null;
      
      // Repeated lists use a different value vector than non-repeated lists.
      
      if ( child.drillField.getType().getMode() == DataMode.REPEATED ) {
        return new RepeatedListNode( batchField, child );
      } else {
        return new ListNode( batchField, child );
      }
    }
    
    @Override
    public String toString( ) {
      StringBuilder buf = new StringBuilder( );
      buf.append( "[" );
      buf.append( getClass( ).getSimpleName() );
      if ( schema != null ) {
        buf.append( " schema=" );
        buf.append( schema.toString() );
      }
      buf.append( " nodes=[" );
      for ( FieldNode node : nodes ) {
        buf.append( node.toString() );
        buf.append( "\n" );
      }
      buf.append( "]]" );
      return buf.toString();
    }
  }
  
  /**
   * The root tuple corresponds to the Jig tuple: it must handle per-record
   * tasks within Jig.
   */
  
  public static class RootTupleNode extends TupleNode {
    
    private DrillRootTupleValue tuple;

    /**
     * Build the set of parse nodes that represent the Drill
     * fields. The parse nodes help us build the machinery needed
     * to access and translate field values.
     * 
     * @param batchSchema
     */
    
    public RootTupleNode( BatchSchema batchSchema ) {
      int fieldCount = batchSchema.getFieldCount();
      for ( int i = 0;  i < fieldCount;  i++ ) {
        addField( batchSchema.getColumn( i ), i );
      }      
    }
    
    /**
     * Build field schema, accessors and values based on
     * the parse nodes for the fields.
     * 
     * @param factory
     */
    
    public void buildFields( FieldValueFactory factory ) {
      
      // The Containers provide variable field values (null or not-null, etc.)
      
      FieldValueContainer containers[] = new FieldValueContainer[nodes.size( )];
      
      // Accessors are bound to vectors and must be rebound on each new batch.
      
      List<VectorAccessor> accessors = new ArrayList<>( );
      
      // Materialized fields are cached, resets clear the cached
      // value on each new tuple.
      
      List<Resetable> resets = new ArrayList<>( );
      
      // Iterate over the nodes that represent the fields.
      
      for ( int i = 0;  i < nodes.size( );  i++ ) {
        FieldNode node = nodes.get( i );
        
        // Build the vector accessor for the field.
        
        VectorAccessor accessor = node.buildField( factory );
        accessor.define( node.schema.nullable(), node.vectorIndex );
        accessors.add( accessor );
        
        // Add any resets needed for materialized values.
        
        if ( node.resetable != null )
          resets.add( node.resetable );
        
        // Build the field value and associated machinery.
        
        node.dataDef.build( factory );
        
        // Add the field values to the set for this tuple.
        
        containers[i] = node.dataDef.container;
      }
      
      // Create the container set used to access field values.
      
      containerSet = new FieldValueContainerSet( containers );

      // Convert the list of vector bindings to an array
      // (for faster runtime access.)
      
      VectorAccessor bindings[] = new VectorAccessor[ accessors.size( ) ];
      accessors.toArray( bindings );
      
      // Create a tuple value visible to the client based on the
      // schema, field values and vector bindings.
      
      tuple = new DrillRootTupleValue( schema, containerSet, bindings );
      
      // Add resets, if any.
      
      if ( ! resets.isEmpty( ) ) {
        tuple.resetable = new Resetable[resets.size()];
        resets.toArray( tuple.resetable );
      }
    }
  }
  
  /**
   * Map fields in Drill have two corresponding parts in Drill. First is
   * a map field (see {@link MapNode}, and the contents of the map,
   * which this node represents. Though Drill calls the field a "map",
   * it is actually a tuple since it has a fixed schema.
   */
  
  public static class MapTupleNode extends TupleNode {

    public MapTupleNode(Collection<MaterializedField> children) {
      int i = 0;
      for ( MaterializedField child : children ) {
        if ( child.getLastName().equals( OFFSETS_FIELD_NAME ) )
          continue;
        addField( child, i++ );
      }
    }
    
    /**
     * A map tuple corresponds to a map field in Drill. Drill
     * materializes such fields as a Java map. Jig presents this
     * as a map of additional field values.
     * 
     * @param objAccessor the accessor that provides the Java map
     * object
     * @param factory
     */
    
    public void buildMaterialized( ObjectAccessor objAccessor, FieldValueFactory factory ) {
      FieldValueContainer containers[] = new FieldValueContainer[nodes.size( )];
      for ( int i = 0;  i < nodes.size( );  i++ ) {
        FieldNode node = nodes.get( i );
        node.buildMaterialized( objAccessor, factory );
        node.dataDef.build( factory );
        containers[i] = node.dataDef.container;
      }
      containerSet = new FieldValueContainerSet( containers );
    }
  }

  /**
   * Represents a field within a Drill record or a member of a 
   * (non-repeated, top-level) Drill map.
   */
  
  public static abstract class FieldNode {

    public final MaterializedField drillField;
    public int vectorIndex;
    public FieldSchemaImpl schema;
    public Resetable resetable;
    public DataDef dataDef;

    public FieldNode(MaterializedField drillField) {
      this.drillField = drillField;
    }

    /**
     * Builds the Jig schema for this field.
     */
    
    public abstract void buildSchema();
    /**
     * Builds the accessors and data definition for top-level
     * fields of each record (tuple).
     * 
     * @param factory
     * @return 
     */
    
    public abstract VectorAccessor buildField( FieldValueFactory factory );
    
    /**
     * Build the accessors and data definition for values that are
     * elements of a structure that Drill materializes as a Java object.
     * For example, maps, arrays and lists. In these cases Drill converts
     * value vectors to Java objects, and the accessors created here
     * work with those materialized objects.
     * 
     * @param objAccessor
     * @param factory
     */
    
    public abstract void buildMaterialized(ObjectAccessor objAccessor,
        FieldValueFactory factory);
    
    @Override
    public String toString( ) {
      StringBuilder buf = new StringBuilder( );
      buf.append( "[" );
      buf.append( getClass( ).getSimpleName() );
      if ( schema != null ) {
        buf.append( " schema=" );
        buf.append( schema.toString() );
      }
      buildContents( buf );
      buf.append( "]" );
      return buf.toString();
    }

    protected void buildContents(StringBuilder buf) { }
    
    protected String getName( ) {
      String name = drillField.getLastName();
      if ( name.equals( DATA_FIELD_NAME ) )
        name = FieldSchema.ELEMENT_NAME;
      return name;
    }
  }
  
  /**
   * Represents a field other than an (element of a) repeated
   * field. This is a bit of a misnomer: in Drill fields are repeated,
   * but in Jig the repetition part is represented as an array.
   * The node here represents each element in such an array.
   */
  
  public static abstract class NonRepeatedNode extends FieldNode {
    
    public NonRepeatedNode(MaterializedField drillField) {
      super(drillField);
    }

    /**
     * Create the schema definition of a non-repeated node. The
     * name is the tail end of the Drill field name. The type is
     * converted from the Drill type. A special case is for the
     * schema of array elements: such fields have no name so we
     * use a dummy name instead.
     */
    
    @Override
    public void buildSchema() {
      String name = getName( );
      DataMode mode = drillField.getDataMode();
      if ( mode == DataMode.REPEATED ) {
        name = FieldSchema.ELEMENT_NAME;
        
        // Nullable is always false for repeated elements.
        // That is, Drill arrays can be empty, but not null.
      }
      DataType type = mapDrillType( drillField.getType().getMinorType() );
      boolean nullable = mode == DataMode.OPTIONAL;
      schema = new FieldSchemaImpl( name, type, nullable );
    }   
    
    private DataType mapDrillType(MinorType minorType) {
      DrillDataType drillType = DrillTypeConversion.getDrillType( minorType );
      if ( ! drillType.isConversionSupported() ) {
        System.err.println( "Warning: Drill type not supported: " + minorType );
        return DataType.UNDEFINED;
      }
      return drillType.jigType;
    }
  }

  /**
   * Represents a simple Drill data type (everything other than a
   * repeated field, a list or a map.)
   */
  
  public static class ScalarNode extends NonRepeatedNode {

    public ScalarNode(MaterializedField drillField) {
      super( drillField );
    }

    @Override
    public VectorAccessor buildField(FieldValueFactory factory) {
      
      // Get Drill's data type used to create the proper vector accessor.
      
      MinorType minorType = drillField.getType().getMinorType();
      
      // Create a simple accessor or a repeated value accessor depending
      // on this field type. (If the mode is REPEATED, then this node
      // represents an element of an array. Otherwise, it represents
      // a simple scalar field.
      
      VectorAccessor accessor;
      if ( drillField.getDataMode() == DataMode.REPEATED ) {
        accessor = VectorAccessor.getElementAccessor( minorType );
      } else {
        accessor = VectorAccessor.getScalarAccessor( minorType );
      }
      
      // Define the data element.
      
      dataDef = new ScalarDef( schema.type( ), schema.nullable( ), accessor );
      return accessor;
    }

    /**
     * Define a field that appears in a materialized structure.
     * Since this node corresponds to a simple type, we just use an
     * accessor that maps "boxed" Java objects to Jig field values.
     */
    
    @Override
    public void buildMaterialized(ObjectAccessor objAccessor,
        FieldValueFactory factory) {
      BoxedAccessor boxed = new DrillBoxedAccessor( objAccessor );
      dataDef = new ScalarDef( schema.type( ), schema.nullable( ), boxed );
    }
  }
  
  /**
   * Represents the a Drill map. A Drill map corresponds to a Jig
   * tuple. The Drill {@link MapVector} materializes the Map as
   * a Java Map, which we present using a specialized map
   * accessor that presents the map as a tuple.
   */
  
  public static class MapNode extends NonRepeatedNode {

    public MapTupleNode tuple;

    public MapNode(MaterializedField drillField) {
      super( drillField );
      tuple = new MapTupleNode( drillField.getChildren() );
    }

    @Override
    public VectorAccessor buildField(FieldValueFactory factory) {
      
      // Create materialized accessors for the members of the Map.
      // Drill declares the members, so we can treat the map as a tuple
      // with known members and types.
      
      CachedObjectAccessor valueAccessor = new CachedObjectAccessor( );
      tuple.buildMaterialized( valueAccessor, factory );
        
      // Create an accessor for Drill's map vector.
      
      MapVectorAccessor accessor = new MapVectorAccessor( );
      
      // The Map vector materializes the map as a Java Map.
      // Cache it for performance.
      
      ReadOnceObjectAccessor objAccessor = new ReadOnceObjectAccessor( accessor );
      resetable = objAccessor;
      
      // Combine the map and element accessors to create the tuple accessor.
      
      DrillMapValueAccessor mapAccessor = new DrillMapValueAccessor( tuple.schema, tuple.containerSet, objAccessor, valueAccessor );
      dataDef = new TupleDef( false, mapAccessor );
      return accessor;
    }

    @Override
    public void buildMaterialized(ObjectAccessor objAccessor,
        FieldValueFactory factory) {
      // TODO Auto-generated method stub
      assert false;
    }
  }
  
//  public static class ImpliedMapNode extends NonRepeatedNode {
//
//    public MapTupleNode tuple;
//
//    public ImpliedMapNode(Collection<MaterializedField> children ) {
//      super( null );
//      tuple = new MapTupleNode( children );
//    }
//    
//    @Override
//    public void buildSchema() {
//      schema = new FieldSchemaImpl( FieldSchema.ELEMENT_NAME, DataType.MAP, false );
//    }
//    
//    @Override
//    public VectorAccessor buildField(FieldValueFactory factory) {
//      assert false;
//      return null;
//    }
//
//    @Override
//    public void buildMaterialized(ObjectAccessor objAccessor,
//        FieldValueFactory factory) {
//      
//      // Create materialized accessors for the members of the Map.
//      // Drill declares the members, so we can treat the map as a tuple
//      // with known members and types.
//      
//      CachedObjectAccessor valueAccessor = new CachedObjectAccessor( );
//      tuple.buildMaterialized( valueAccessor, factory );
//        
//      // Combine the map and element accessors to create the tuple accessor.
//      
//      DrillMapValueAccessor mapAccessor = new DrillMapValueAccessor( tuple.schema, tuple.containerSet, objAccessor, valueAccessor );
//      dataDef = new TupleDef( false, mapAccessor );
//    }
//  }
  
  /**
   * Corresponds to the "virtual" Jig array field that wraps the
   * actual repeated Drill field.
   */
  
  public static class RepeatedNode extends FieldNode {

    private FieldNode element;
    
    public RepeatedNode(MaterializedField drillField, FieldNode element) {
      super( drillField );
      this.element = element;
    }

    @Override
    public void buildSchema() {
      element.buildSchema( );
      schema = new ArrayFieldSchemaImpl( getName( ), false, element.schema );
    }

    @Override
    public VectorAccessor buildField(FieldValueFactory factory) {
      
      // Build the type-specific vector element accessor for the
      // repeated elements. (In Jig, we treat the set of repeated elements as
      // an array, and each item as an array element.)
      
      VectorAccessor elementAccessor = element.buildField( factory );
      RepeatedVectorAccessor arrayAccessor = new RepeatedVectorAccessor( (DrillElementAccessor) elementAccessor );
      
      // Combine the list and element.
      
      dataDef = new ListDef( false, element.dataDef, arrayAccessor );
      return elementAccessor;
    }

    @Override
    public void buildMaterialized(ObjectAccessor objAccessor,
        FieldValueFactory factory) {
      JavaListAccessor listAccessor = new JavaListAccessor( objAccessor );
      element.buildMaterialized( ((ObjectAccessor) listAccessor.memberAccessor()), factory);
      dataDef = new ListDef( false, element.dataDef, listAccessor );
    }

    @Override
    protected void buildContents(StringBuilder buf) {
      if ( element != null ) {
        buf.append( " element=" );
        buf.append( element.toString() );
      }
    }
  }
  /**
   * Represents a repeated map, backed by a <code>RepeatedMapVector</code>.
   * The vector materializes the repeated maps as a Java list. Java
   * accessors then retrieve the individual maps and the fields within
   * the maps. Since Drill provides a map schema, the repeated map is,
   * in Jig's terms, a repeated tuple.
   */
  
  public static class RepeatedMapNode extends FieldNode {

    public MapTupleNode tuple;

    public RepeatedMapNode(MaterializedField batchField) {
      super( batchField );
      tuple = new MapTupleNode( drillField.getChildren() );
    }

    @Override
    public void buildSchema() {
      FieldSchema element = new FieldSchemaImpl( FieldSchema.ELEMENT_NAME, DataType.MAP, false );
      schema = new ArrayFieldSchemaImpl( getName( ), false, element );
    }

    @Override
    public VectorAccessor buildField(FieldValueFactory factory) {
      
      // Create the acccessor for Drill's repeated map vector
      
      RepeatedMapVectorAccessor accessor = new RepeatedMapVectorAccessor( );
      
      // Drill returns the repeated maps as a Java list. Cache it.
      
      ReadOnceObjectAccessor objAccessor = new ReadOnceObjectAccessor( accessor );
      resetable = objAccessor;
      
      // Build the materialized list accessor.
      
      buildMaterialized( objAccessor, factory );
      return accessor;
    }

    @Override
    public void buildMaterialized(ObjectAccessor objAccessor,
        FieldValueFactory factory) {
      
      // The repeated map is materialized as a list. Create the
      // list accessor.
      
      JavaListAccessor listAccessor = new JavaListAccessor( objAccessor );
      
      // Build the materialized member accessors.
      
      CachedObjectAccessor valueAccessor = new CachedObjectAccessor( );
      tuple.buildMaterialized( valueAccessor, factory );
      
      // Present each map as a tuple
      
      DrillMapValueAccessor mapAccessor = new DrillMapValueAccessor( tuple.schema, tuple.containerSet,
          (ObjectAccessor) listAccessor.memberAccessor(), valueAccessor );      
      DataDef elementDef = new TupleDef( false, mapAccessor );
      
      // Define the list and its element
      
      dataDef = new ListDef( false, elementDef, listAccessor );
    }
  }

  /**
   * Represents a Drill non-repeated list backed by a
   * <code>ListVector</code>. Drill materializes the value as a
   * Java <code>List</code> with elements of the declared element
   * type. Jig uses a Java list accessor along with a type-specific
   * element accessor. 
   */
  
  public static abstract class AbstractListNode extends FieldNode {

    private FieldNode element;

    public AbstractListNode(MaterializedField drillField, FieldNode element) {
      super( drillField );
      this.element = element;
    }

    @Override
    public void buildSchema() {
      element.buildSchema();
      schema = new ArrayFieldSchemaImpl( getName( ), false, element.schema );
    }

    @Override
    public VectorAccessor buildField(FieldValueFactory factory) {
      
      // Vector accessor
      
      VectorAccessor accessor = buildVectorAccessor( );
      
      // Cached the materialized list object.
      
      ReadOnceObjectAccessor objAccessor = new ReadOnceObjectAccessor( (ObjectAccessor) accessor );
      resetable = objAccessor;
      
      buildMaterialized( objAccessor, factory );
      return accessor;
    }

    protected abstract VectorAccessor buildVectorAccessor();

    @Override
    public void buildMaterialized(ObjectAccessor objAccessor,
        FieldValueFactory factory) {
      
      // Accessor for the materialized list
      
      JavaListAccessor javaAccessor = new JavaListAccessor( objAccessor, factory );
      
      // Build the element (using the materialized list)
      
      element.buildMaterialized(((ObjectAccessor) javaAccessor.memberAccessor()), factory);
      
      // Combine the list and element data.
      
      dataDef = new ListDef( false, element.dataDef, javaAccessor );
    }
  }
  
  public static class ListNode extends AbstractListNode {
    
    public ListNode(MaterializedField drillField, FieldNode element) {
      super( drillField, element );
    }

    @Override
    protected VectorAccessor buildVectorAccessor() {
      return new ListVectorAccessor( );
    }
  }
  
  /**
   * Represents a repeated list, backed by a <code>RepeatedListVector</code>.
   * Drill materializes each column value as a Java <code>List<code> of
   * <code>List</code> of values, where the type
   * of the item depends on the type of items within the list. Jig uses a
   * Java list accessor for the outer list, then an element accessor that
   * is also a Java list accessor for the inner list, finally with
   * a type-specific member accessor to pick
   * apart the materialized value.
   */
  
  public static class RepeatedListNode extends AbstractListNode {
    
    public RepeatedListNode(MaterializedField drillField, FieldNode element) {
      super( drillField, element );
    }

    @Override
    protected VectorAccessor buildVectorAccessor() {
      return new RepeatedListVectorAccessor( );
    }
  }
  
  private BatchSchema batchSchema;

  public TupleBuilder(BatchSchema schema) {
    batchSchema = schema;
  }

  /**
   * Build a Jig tuple and tuple schema. Creates a series of parse nodes to
   * represent the Drill structure, then visits the structure to build the
   * Jig schema, the Jig accessors and the Jig field values. Finally assembles
   * the whole mess into a single Jig tuple value.
   * @return
   */
  
  public DrillRootTupleValue build() {
    dumpSchema( );
    RootTupleNode root = new RootTupleNode( batchSchema );
       
    FieldValueFactory factory = new DrillFieldValueFactory( );
    root.buildFields( factory );
    System.out.println( "Jig Schema: " );
    System.out.println( root.tuple.schema().toString() );
    return root.tuple;
  }

  private void dumpSchema( ) {
    System.out.println( "Drill Schema:" );
    for ( MaterializedField field : batchSchema ) {
      dumpField( "  ", field );
    }
    
  }

  private void dumpField(String indent, MaterializedField field) {
    System.out.print( indent );
    System.out.println( field.toString() );
    for ( MaterializedField child : field.getChildren() ) {
      dumpField( indent + "  ", child );
    }
  }
}