package org.apache.drill.jig.direct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.jig.accessor.FieldAccessor.Resetable;
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
import org.apache.drill.jig.direct.DrillTupleValue.DrillRootTupleValue;
import org.apache.drill.jig.direct.DrillTypeConversion.DrillDataType;
import org.apache.drill.jig.direct.VectorAccessor.DrillElementAccessor;
import org.apache.drill.jig.types.FieldValueFactory;

public class TupleBuilder
{
  public static abstract class TupleNode {
    
    protected final TupleSchemaImpl schema = new TupleSchemaImpl( );
    protected final List<FieldNode> nodes = new ArrayList<>( );
    FieldValueContainerSet containerSet;
    
    protected void addField( MaterializedField batchField ) {
      FieldNode node = makeField( batchField );
      node.buildSchema( );
      nodes.add( node );
      schema.add( node.schema );
    }
    
    private FieldNode makeField(MaterializedField batchField) {
      if ( batchField.getDataMode() == DataMode.REPEATED ) {        
        return new RepeatedNode( batchField, buildNode( batchField ) );
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

  }
  
  public static class RootTupleNode extends TupleNode {
    
    private DrillRootTupleValue tuple;

    public RootTupleNode( BatchSchema batchSchema ) {
      int fieldCount = batchSchema.getFieldCount();
      for ( int i = 0;  i < fieldCount;  i++ ) {
        addField( batchSchema.getColumn( i ) );
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
  
  public static class MapTupleNode extends TupleNode {

    private DrillTupleValue tuple;

    public MapTupleNode(Collection<MaterializedField> children) {
      for ( MaterializedField child : children )
        addField( child );
    }
    
    @Override
    public void build( FieldValueFactory factory ) {
      super.build( factory );
      tuple = new DrillTupleValue( schema, containerSet );
    }
  }
  
  public static abstract class FieldNode {

    public final MaterializedField drillField;
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
  }
  
  public static abstract class NonRepeatedNode extends FieldNode {
    
    public NonRepeatedNode(MaterializedField drillField) {
      super(drillField);
    }

    @Override
    public void buildSchema() {
      String name = drillField.getName();
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
      if ( ! drillType.isConversionSupported() )
        return DataType.UNDEFINED;
      return drillType.jigType;
    }
  }

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
  
  public static class MapNode extends NonRepeatedNode {

    public MapTupleNode tuple;
    private DrillMapAccessor accessor;

    public MapNode(MaterializedField drillField) {
      super( drillField );
      tuple = new MapTupleNode( drillField.getChildren() );
    }

    @Override
    public void build(FieldValueFactory factory) {
      tuple.build( factory );
      accessor = new DrillMapAccessor( tuple.tuple );
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
  }
  
  public static class RepeatedNode extends FieldNode {

    private FieldNode element;
    private DrillRepeatedVectorAccessor arrayAccessor;
    
    public RepeatedNode(MaterializedField drillField, FieldNode element) {
      super( drillField );
      this.element = element;
    }

    @Override
    public void buildSchema() {
      element.buildSchema( );
      schema = new ArrayFieldSchemaImpl( drillField.getName(), false, element.schema );
    }

    @Override
    public void build(FieldValueFactory factory) {
      element.build( factory );
      arrayAccessor = new DrillRepeatedVectorAccessor( (DrillElementAccessor) element.getAccessor( ) );
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
    
  }
  
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

  public DrillRootTupleValue build() {
    RootTupleNode root = new RootTupleNode( batchSchema );
       
    FieldValueFactory factory = new FieldValueFactory( );
    root.build( factory );
    return root.tuple;
    
//    List<Resetable> resets = new ArrayList<>( );
//    root.gatherResetable( resets );
//    
//    TupleSchemaImpl schema = new TupleSchemaImpl( );
//    FieldValueContainer containers[] = new FieldValueContainer[n];
//    int fieldCount = batchSchema.getFieldCount();
//    for ( int i = 0;  i < fieldCount;  i++ ) {
//      FieldNode node = makeField( batchSchema.getColumn( i ) );
//      node.buildSchema( );
//      schema.add( node.schema );
//      node.build( );
//      if ( node.resetable != null )
//        resets.add( node.resetable );
//      containers[i] = node.dataDef.container;
//    }
//    FieldValueContainerSet containerSet = new FieldValueContainerSet( containers );
//    DrillTupleValue tuple = new DrillTupleValue( schema, containerSet );
//    if ( ! resets.isEmpty() ) {
//      tuple.resetable = new Resetable[resets.size()];
//      resets.toArray( tuple.resetable );
//    }
//    return tuple;
  }

//  private DataType mapDrillType(MinorType minorType) {
//    DrillDataType drillType = DrillTypeConversion.getDrillType( minorType );
//    if ( ! drillType.isConversionSupported() )
//      return DataType.UNDEFINED;
//    return drillType.jigType;
//    switch ( minorType ) {
//    case BIGINT:
//      return Type.LONG;
//    case BIT:
//      return Type.BOOLEAN;
//    case VARCHAR:
//      return Type.STRING;
//    case FLOAT8:
//      return Type.DOUBLE;
//    case NULL:
//      return Type.NULL;
//    case DATE:
//      break;
//    case DECIMAL18:
//      break;
//    case DECIMAL28DENSE:
//      break;
//    case DECIMAL28SPARSE:
//      break;
//    case DECIMAL38DENSE:
//      break;
//    case DECIMAL38SPARSE:
//      break;
//    case DECIMAL9:
//      break;
//    case FIXED16CHAR:
//      break;
//    case FIXEDBINARY:
//      break;
//    case FIXEDCHAR:
//      break;
//    case FLOAT4:
//      break;
//    case GENERIC_OBJECT:
//      break;
//    case INT:
//      break;
//    case INTERVAL:
//      break;
//    case INTERVALDAY:
//      break;
//    case INTERVALYEAR:
//      break;
//    case LATE:
//      break;
//    case LIST:
//      break;
//    case MAP:
//      break;
//    case MONEY:
//      break;
//    case SMALLINT:
//      break;
//    case TIME:
//      break;
//    case TIMESTAMP:
//      break;
//    case TIMESTAMPTZ:
//      break;
//    case TIMETZ:
//      break;
//    case TINYINT:
//      break;
//    case UINT1:
//      break;
//    case UINT2:
//      break;
//    case UINT4:
//      break;
//    case UINT8:
//      break;
//    case UNION:
//      break;
//    case VAR16CHAR:
//      break;
//    case VARBINARY:
//      break;
//    default:
//      throw new IllegalStateException( "Unsupported Minor Type: " + minorType.name() );
//    }
//  }

//  private Cardinality mapDrillCardinality(DataMode dataMode) {
//    switch( dataMode ) {
//    case OPTIONAL:
//      return Cardinality.OPTIONAL;
//    case REPEATED:
//      return Cardinality.REPEATED;
//    case REQUIRED:
//      return Cardinality.REQUIRED;
//    default:
//      throw new IllegalStateException( "Unexpected DataMode: " + dataMode.name() );
//    }
//  }
  
//  private DrillFieldAccessor[] buildAccesors(DrillTupleSchema schema) {
//    int fieldCount = schema.count();
//    DrillFieldAccessor accessors[] = new DrillFieldAccessor[ fieldCount ];
//    for ( int i = 0;  i < fieldCount;  i++ ) {
//      accessors[i] = buildAccessor( schema.getDrillField( i ) );
//    }
//    return accessors;
//  }
//  
//  private static Class<? extends DrillFieldAccessor> accessors[ ] =
//      buildAccessorTable( );

//  private static Class<? extends DrillFieldAccessor>[] buildAccessorTable() {
//    @SuppressWarnings("unchecked")
//    Class<? extends DrillFieldAccessor> table[] =
//        new Class[ MinorType.values().length ];
//    table[MinorType.BIT.ordinal( )] = DrillFieldAccessor.BitVectorAccessor.class;
//    table[MinorType.INT.ordinal( )] = DrillFieldAccessor.IntVectorAccessor.class;
//    table[MinorType.BIGINT.ordinal( )] = DrillFieldAccessor.BigIntVectorAccessor.class;
//    table[MinorType.FLOAT4.ordinal( )] = DrillFieldAccessor.Float4VectorAccessor.class;
//    table[MinorType.FLOAT8.ordinal( )] = DrillFieldAccessor.Float8VectorAccessor.class;
//    table[MinorType.VARCHAR.ordinal( )] = DrillFieldAccessor.VarCharVectorAccessor.class;
//    return table;
//  }

//  // TODO: Create a table, and do selection based on Drill types,
//  // not API types.
//  
//  private DrillFieldAccessor buildAccessor(DrillFieldSchema field) {
//    Class<? extends DrillFieldAccessor> accessorClass =
//        accessors[ field.drillType.ordinal( ) ];
//    if ( accessorClass == null )
//      accessorClass = DrillFieldAccessor.NullAccessor.class;
//    try {
//      return accessorClass.newInstance();
//    } catch (InstantiationException e) {
//      throw new IllegalStateException( e );
//    } catch (IllegalAccessException e) {
//      throw new IllegalStateException( e );
//    }
//    switch( field.getType() ) {
//    case DOUBLE:
//      return new DrillFieldAccessor.DrillFloat8Accessor( );
//    case LONG:
//      return new DrillFieldAccessor.DrillBigIntAccessor( );
//    case STRING:
//      return new DrillFieldAccessor.DrillVarCharAccessor( );
//    case ANY:
//      break;
//    case BIG_DECIMAL:
//      break;
//    case BOOLEAN:
//      break;
//    case NULL:
//      break;
//    case TUPLE:
//      break;
//    default:
//      throw new IllegalStateException( "Unsupported Type: " + field.getType().getDisplayName() );
//    }
//  }

}