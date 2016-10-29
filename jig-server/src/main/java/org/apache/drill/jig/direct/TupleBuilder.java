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
    
    protected void addField( MaterializedField batchField, int vectorIndex ) {
      FieldNode node = makeField( batchField );
      node.vectorIndex = vectorIndex;
      node.buildSchema( );
      schema.add( node.schema );
      nodes.add( node );
    }
    
    private FieldNode makeField(MaterializedField batchField) {
      if ( batchField.getDataMode() == DataMode.REPEATED ) {        
        MinorType drillType = batchField.getType().getMinorType();
        if ( drillType == MinorType.MAP ) {
          return new RepeatedMapNode( batchField );
        } else {
          return new RepeatedNode( batchField, buildNode( batchField ) );
        }
      } else {
        return buildNode( batchField );
      }
    }
   
    private FieldNode buildNode( MaterializedField batchField ) {
      MinorType drillType = batchField.getType().getMinorType();
      if ( drillType == MinorType.MAP ) {
        return new MapNode( batchField );
      } else if ( drillType == MinorType.LIST ) {
        return buildListNode( batchField );
      } else {
        return new ScalarNode( batchField );
      }
    }

    private FieldNode buildListNode(MaterializedField batchField) {
      Collection<MaterializedField> children = batchField.getChildren();
      FieldNode child;
      if ( children.size() == 1 )
        child = makeField( children.iterator().next() );
      else
        child = new ImpliedMapNode( children );
      return new ListNode( batchField, child );
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

    public RootTupleNode( BatchSchema batchSchema ) {
      int fieldCount = batchSchema.getFieldCount();
      for ( int i = 0;  i < fieldCount;  i++ ) {
        addField( batchSchema.getColumn( i ), i );
      }      
    }
    
    public void buildFields( FieldValueFactory factory ) {
      FieldValueContainer containers[] = new FieldValueContainer[nodes.size( )];
      List<VectorAccessor> accessors = new ArrayList<>( );
      List<Resetable> resets = new ArrayList<>( );
      for ( int i = 0;  i < nodes.size( );  i++ ) {
        FieldNode node = nodes.get( i );
        accessors.add( node.buildField( factory ) );
        if ( node.resetable != null )
          resets.add( node.resetable );
        node.dataDef.build( factory );
        containers[i] = node.dataDef.container;
      }
      containerSet = new FieldValueContainerSet( containers );

      VectorAccessor bindings[] = new VectorAccessor[ accessors.size( ) ];
      accessors.toArray( bindings );
      
      tuple = new DrillRootTupleValue( schema, containerSet, bindings );
      
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
        if ( child.getLastName().equals( "$offsets$" ) )
          continue;
        addField( child, i++ );
      }
    }
    
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
     * Builds the accessors and data definition.
     * 
     * @param factory
     * @return 
     */
    
    public abstract VectorAccessor buildField( FieldValueFactory factory );
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

    @Override
    public void buildSchema() {
      String name = drillField.getLastName();
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

    private VectorAccessor accessor;

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
      
      if ( drillField.getDataMode() == DataMode.REPEATED ) {
        accessor = VectorAccessor.getElementAccessor( minorType );
      } else {
        accessor = VectorAccessor.getScalarAccessor( minorType );
      }
      accessor.bindSchema( schema );
      
      // Define the data element.
      
      dataDef = new ScalarDef( schema.type( ), schema.nullable( ), accessor );
      return accessor;
    }

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
      accessor.bindSchema( schema );
      
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
  
  public static class ImpliedMapNode extends NonRepeatedNode {

    public ImpliedMapNode(Collection<MaterializedField> children ) {
      super(null);
    }

    @Override
    public VectorAccessor buildField(FieldValueFactory factory) {
      assert false;
      return null;
    }

    @Override
    public void buildMaterialized(ObjectAccessor objAccessor,
        FieldValueFactory factory) {
      assert false;
    }
  }
  
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
      schema = new ArrayFieldSchemaImpl( drillField.getLastName(), false, element.schema );
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
    }

    @Override
    protected void buildContents(StringBuilder buf) {
      if ( element != null ) {
        buf.append( " element=" );
        buf.append( element.toString() );
      }
    }
  }
  
  public static class RepeatedMapNode extends FieldNode {

    public MapTupleNode tuple;

    public RepeatedMapNode(MaterializedField batchField) {
      super( batchField );
      tuple = new MapTupleNode( drillField.getChildren() );
    }

    @Override
    public void buildSchema() {
      FieldSchema element = new FieldSchemaImpl( FieldSchema.ELEMENT_NAME, DataType.MAP, false );
      schema = new ArrayFieldSchemaImpl( drillField.getLastName(), false, element );
    }

    @Override
    public VectorAccessor buildField(FieldValueFactory factory) {
      
      // Create the acccessor for Drill's repeated map vector
      
      RepeatedMapVectorAccessor accessor = new RepeatedMapVectorAccessor( );
      accessor.bindSchema( schema );
      
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
   * Represents a Drill list node.
   */
  
  public static class ListNode extends FieldNode {

    private FieldNode element;

    public ListNode(MaterializedField drillField, FieldNode element) {
      super( drillField );
      this.element = element;
    }

    @Override
    public void buildSchema() {
      element.buildSchema();
      schema = new ArrayFieldSchemaImpl( drillField.getLastName(), false, element.schema );
    }

    @Override
    public VectorAccessor buildField(FieldValueFactory factory) {
      
      // Vector accessor
      
      ListVectorAccessor accessor = new ListVectorAccessor( );
      accessor.bindSchema( schema );
      
      // Cached the materialized list object.
      
      ReadOnceObjectAccessor objAccessor = new ReadOnceObjectAccessor( accessor );
      resetable = objAccessor;
      
      // Accessor for the materialized list
      
      JavaListAccessor javaAccessor = new JavaListAccessor( objAccessor, factory );
      
      // Build the element (using the materialized list)
      
      element.buildMaterialized(((ObjectAccessor) javaAccessor.memberAccessor()), factory);
      
      // Combine the list and element data.
      
      dataDef = new ListDef( false, element.dataDef, javaAccessor );
      return accessor;
    }

    @Override
    public void buildMaterialized(ObjectAccessor objAccessor,
        FieldValueFactory factory) {
      assert false;
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
    RootTupleNode root = new RootTupleNode( batchSchema );
       
    FieldValueFactory factory = new DrillFieldValueFactory( );
    root.buildFields( factory );
    return root.tuple;
  }
}