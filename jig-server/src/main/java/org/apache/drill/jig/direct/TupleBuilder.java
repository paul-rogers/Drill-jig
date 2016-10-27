package org.apache.drill.jig.direct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.jig.accessor.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.Resetable;
import org.apache.drill.jig.accessor.JavaMapAccessor;
import org.apache.drill.jig.accessor.ReadOnceObjectAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.impl.ArrayFieldSchemaImpl;
import org.apache.drill.jig.api.impl.DataDef;
import org.apache.drill.jig.api.impl.DataDef.*;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.container.FieldValueContainer;
import org.apache.drill.jig.container.FieldValueContainerSet;
import org.apache.drill.jig.direct.DirectTupleValue.DrillRootTupleValue;
import org.apache.drill.jig.direct.DrillTypeConversion.DrillDataType;
import org.apache.drill.jig.direct.VectorAccessor.DrillElementAccessor;
import org.apache.drill.jig.types.FieldValueFactory;

/**
 * Build a Jig schema and tuple implementation given a Drill schema.
 * Maps Jig field values to Drill vector accessors. Creates "virtual"
 * fields to represent Drill repeated types (presented as an array)
 * and Drill maps (presented as a tuple.)
 */

public class TupleBuilder
{
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
      nodes.add( node );
      schema.add( node.schema );
    }
    
    private FieldNode makeField(MaterializedField batchField) {
      if ( batchField.getDataMode() == DataMode.REPEATED ) {        
        MinorType drillType = batchField.getType().getMinorType();
        if ( drillType == MinorType.MAP ) {
          return new RepeatedNode( batchField, new MapElementNode( batchField ) );
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
      if ( children.size() != 1 )
        throw new IllegalStateException( "List field should have one child, found " + children.size() );
      return new ListNode( batchField, makeField( children.iterator().next() ) );
    }
    
    public void build( FieldValueFactory factory ) {
      FieldValueContainer containers[] = new FieldValueContainer[nodes.size( )];
      for ( int i = 0;  i < nodes.size( );  i++ ) {
        FieldNode node = nodes.get( i );
        node.build( factory );
        containers[i] = node.dataDef.container;
      }
      containerSet = new FieldValueContainerSet( containers );
   }
    
    public void gatherResetable( List<Resetable> resets ) {
      for ( int i = 0;  i < nodes.size( );  i++ ) {
        nodes.get( i ).gatherResetable( resets );
      }
    }
    
    public void gatherVectorBindings( List<VectorAccessor> accessors ) {
      for ( int i = 0;  i < nodes.size( );  i++ ) {
        nodes.get( i ).gatherVectorBindings( accessors );
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

    public RootTupleNode( BatchSchema batchSchema ) {
      int fieldCount = batchSchema.getFieldCount();
      for ( int i = 0;  i < fieldCount;  i++ ) {
        addField( batchSchema.getColumn( i ), i );
      }      
    }
    
    @Override
    public void build( FieldValueFactory factory ) {
      super.build( factory );
      
      List<VectorAccessor> accessors = new ArrayList<>( );
      gatherVectorBindings( accessors );
      VectorAccessor bindings[] = new VectorAccessor[ accessors.size( ) ];
      accessors.toArray( bindings );
      
      tuple = new DrillRootTupleValue( schema, containerSet, bindings );
      
      List<Resetable> resets = new ArrayList<>( );
      gatherResetable( resets );
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

    private DirectTupleValue tuple;

    public MapTupleNode(Collection<MaterializedField> children) {
      int i = 0;
      for ( MaterializedField child : children )
        addField( child, i++ );
    }
    
    @Override
    public void build( FieldValueFactory factory ) {
      super.build( factory );
      tuple = new DirectTupleValue( schema, containerSet );
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

    public abstract void gatherVectorBindings(List<VectorAccessor> accessors);

    public void gatherResetable(List<Resetable> resets) {
      if ( resetable != null )
        resets.add( resetable );
    }

    public abstract void build( FieldValueFactory factory );
    public abstract void buildSchema();
    public abstract VectorAccessor getAccessor( );
    
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
    public void build(FieldValueFactory factory) {
      MinorType minorType = drillField.getType().getMinorType();
      if ( drillField.getDataMode() == DataMode.REPEATED ) {
        accessor = VectorAccessor.getElementAccessor( minorType );
      } else {
        accessor = VectorAccessor.getScalarAccessor( minorType );
      }
      accessor.bindSchema( schema );
      dataDef = new ScalarDef( schema.type( ), schema.nullable( ), accessor );
      dataDef.build( factory );
    }

    @Override
    public void gatherVectorBindings(List<VectorAccessor> accessors) {
      accessors.add( accessor );
    }

    @Override
    public VectorAccessor getAccessor() {
      return accessor;
    }
  }
  
  /**
   * Represents the a Drill map. A Drill map corresponds to a Jig
   * tuple. This node represents the "virtual" Jig map field,
   * though Drill has only the contained fields.
   */
  
  public static class MapNode extends NonRepeatedNode {

    public MapTupleNode tuple;
    private MapVectorAccessor accessor;

    public MapNode(MaterializedField drillField) {
      super( drillField );
      tuple = new MapTupleNode( drillField.getChildren() );
    }

    @Override
    public void build(FieldValueFactory factory) {
      tuple.build( factory );
      accessor = new MapVectorAccessor( tuple.tuple );
      dataDef = new TupleDef( false, accessor, tuple.tuple );
      dataDef.build( factory );
    }

    @Override
    public void gatherVectorBindings(List<VectorAccessor> accessors) {
      tuple.gatherVectorBindings( accessors );
    }
    
    @Override
    public void gatherResetable(List<Resetable> resets) {
      super.gatherResetable(resets);
      tuple.gatherResetable(resets);
    }

    @Override
    public VectorAccessor getAccessor() {
      assert false;
      return null;
    }
    
    @Override
    protected void buildContents(StringBuilder buf) {
      if ( tuple != null ) {
        buf.append( " tuple=" );
        buf.append( tuple.toString() );
      }
    }
  }
  
  public static class MapElementNode extends NonRepeatedNode {
    
    private RepeatedMapVectorElementAccessor accessor;

    public MapElementNode(MaterializedField batchField) {
      super( batchField );
    }

    @Override
    public void buildSchema() {
      schema = new FieldSchemaImpl( FieldSchema.ELEMENT_NAME, DataType.MAP, false );
    }

    @Override
    public void build(FieldValueFactory factory) {
      accessor = new RepeatedMapVectorElementAccessor( );
      ReadOnceObjectAccessor objAccessor = new ReadOnceObjectAccessor( accessor );
      MapValueAccessor mapAccessor = new JavaMapAccessor( objAccessor, factory );
      dataDef = new MapDef( false, mapAccessor );
      dataDef.build(factory);
    }

    @Override
    public void gatherVectorBindings(List<VectorAccessor> accessors) {
      accessors.add( accessor );
    }

    @Override
    public VectorAccessor getAccessor() {
      return accessor;
    }
  
  }

  
  /**
   * Corresponds to the "virtual" Jig array field that wraps the
   * actual repeated Drill field.
   */
  
  public static class RepeatedNode extends FieldNode {

    private FieldNode element;
    private RepeatedVectorAccessor arrayAccessor;
    
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
    public void build(FieldValueFactory factory) {
      element.build( factory );
      arrayAccessor = new RepeatedVectorAccessor( (DrillElementAccessor) element.getAccessor( ) );
      dataDef = new ListDef( false, element.dataDef, arrayAccessor );
      dataDef.build( factory );
    }

    @Override
    public void gatherVectorBindings(List<VectorAccessor> accessors) {
      element.gatherVectorBindings(accessors);
    }

    @Override
    public VectorAccessor getAccessor() {
      return arrayAccessor;
    }   
    
    @Override
    protected void buildContents(StringBuilder buf) {
      if ( element != null ) {
        buf.append( " element=" );
        buf.append( element.toString() );
      }
    }
  }
  
//  public static class RepeatedMapNode extends FieldNode {
//
//    private RepeatedMapVectorElementAccessor mapAccessor;
//
//    public RepeatedMapNode(MaterializedField batchField) {
//      super( batchField );
//    }
//
//    @Override
//    public void buildSchema() {
//      FieldSchema element = new FieldSchemaImpl( FieldSchema.ELEMENT_NAME, DataType.MAP, false );
//      schema = new ArrayFieldSchemaImpl( drillField.getLastName(), false, element );
//    }
//
//    @Override
//    public void build(FieldValueFactory factory) {
//      mapAccessor = new RepeatedMapVectorElementAccessor( );
//      MapValueAccessor mapAccessor = new JavaMapAccessor( mapAccessor, factory );
//      DataDef elementDef = new MapDef( false, mapAccessor );
//      
//      arrayAccessor = new RepeatedVectorAccessor( (DrillElementAccessor) element.getAccessor( ) );
//      dataDef = new ListDef( false, element.dataDef, arrayAccessor );
//      dataDef.build( factory );
//    }
//
//    @Override
//    public void gatherVectorBindings(List<VectorAccessor> accessors) {
//      accessors.add( mapAccessor );
//    }
//
//    @Override
//    public VectorAccessor getAccessor() {
//      return mapAccessor;
//    }
//  
//  }

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
    public void gatherVectorBindings(List<VectorAccessor> accessors) {
      // TODO Auto-generated method stub     
    }

    @Override
    public void build(FieldValueFactory factory) {
      // TODO Auto-generated method stub    
    }

    @Override
    public void buildSchema() {
      // TODO Auto-generated method stub      
    }

    @Override
    public VectorAccessor getAccessor() {
      // TODO Auto-generated method stub
      return null;
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
       
    FieldValueFactory factory = new FieldValueFactory( );
    root.build( factory );
    return root.tuple;
  }
}