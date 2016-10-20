package org.apache.drill.jig.direct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.jig.accessor.ReadOnceObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.Resetable;
import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.impl.ArrayFieldSchemaImpl;
import org.apache.drill.jig.api.impl.DataDef;
import org.apache.drill.jig.api.impl.DataDef.*;
import org.apache.drill.jig.container.FieldValueContainer;
import org.apache.drill.jig.container.FieldValueContainerSet;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.direct.DrillTupleSchema.DrillFieldSchema;
import org.apache.drill.jig.direct.DrillTypeConversion.DrillDataType;
import org.apache.drill.jig.direct.VectorAccessor.DrillElementAccessor;
import org.apache.drill.jig.serde.deserializer.BufferTupleValue;
import org.apache.drill.jig.serde.deserializer.TupleBuilder.ValueNode;
import org.apache.drill.jig.types.FieldValueFactory;

public class DrillTupleSchemaBuilder
{
  public static abstract class DrillNode {

    public final MaterializedField drillField;
    public FieldSchemaImpl schema;
    public Resetable resetable;
    public DataDef dataDef;

    public DrillNode(MaterializedField drillField) {
      this.drillField = drillField;
    }

    public abstract void build( FieldValueFactory factory );

    public abstract void buildSchema();
  }
  
  public static abstract class NonRepeatedNode extends DrillNode {
    
    public NonRepeatedNode(MaterializedField drillField) {
      super(drillField);
    }

    @Override
    public void buildSchema() {
      String name = drillField.getName();
      DataMode mode = drillField.getDataMode();
      if ( mode == DataMode.REPEATED ) {
        name = FieldSchema.ELEMENT_NAME;
        
        // Nullable always false for repeated elements.
        // That is, Drill arrays can be empty, but not null.
      }
      DataType type = mapDrillType( drillField.getType().getMinorType() );
      boolean nullable = drillField.getDataMode() == DataMode.OPTIONAL;
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

    private DrillElementAccessor accessor;

    public ScalarNode(MaterializedField drillField) {
      super( drillField );
    }

    @Override
    public void build(FieldValueFactory factory) {
      accessor = VectorAccessor.getElementAccessor( drillField.getType().getMinorType() );
      dataDef = new ScalarDef( schema.type( ), schema.nullable( ), accessor );
    }
  }
  
  public static class MapNode extends NonRepeatedNode {

    public List<DrillNode> children = new ArrayList<>( );

    public MapNode(MaterializedField drillField) {
      super( drillField );
    }

    @Override
    public void build(FieldValueFactory factory) {
      DrillMapAccessor mapAccessor = new DrillMapAccessor( );
      
      // The buffer structure accessor deserializes the structure to a Java Map,
      // or array which is presented as a Java object.
      
      ReadOnceObjectAccessor cache = new ReadOnceObjectAccessor( mapAccessor );
      
      // The read once accessor caches the value for each tuple,
      // add a reset to clear the cached value on each new tuple.
      
      resetable = cache;
    }
    
  }
  
  public static class RepeatedNode extends DrillNode {

    private DrillNode element;
    
    public RepeatedNode(MaterializedField drillField, DrillNode element) {
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
      DatD
      dataDef = new ListDef( );
    }
    
  }
  
  public static class ListNode extends DrillNode {

    private DrillNode element;

    public ListNode(MaterializedField drillField, DrillNode element) {
      super( drillField );
      this.element = element;
    }
    
  }
  
  private BatchSchema batchSchema;

  public DrillTupleSchemaBuilder(BatchSchema schema) {
    batchSchema = schema;
  }

  public DrillTupleValue build() {
    TupleSchemaImpl schema = new TupleSchemaImpl( );
    List<Resetable> resets = new ArrayList<>( );
    FieldValueContainer containers[] = new FieldValueContainer[n];
    int fieldCount = batchSchema.getFieldCount();
    for ( int i = 0;  i < fieldCount;  i++ ) {
      DrillNode node = makeField( batchSchema.getColumn( i ) );
      node.buildSchema( );
      schema.add( node.schema );
      node.build( );
      if ( node.resetable != null )
        resets.add( node.resetable );
      containers[i] = node.dataDef.container;
    }
    FieldValueContainerSet containerSet = new FieldValueContainerSet( containers );
    DrillTupleValue tuple = new DrillTupleValue( schema, containerSet );
    if ( ! resets.isEmpty() ) {
      tuple.resetable = new Resetable[resets.size()];
      resets.toArray( tuple.resetable );
    }
    return tuple;
  }

  private DrillNode makeField(MaterializedField batchField) {
    if ( batchField.getDataMode() == DataMode.REPEATED ) {        
      return new RepeatedNode( batchField, buildNode( batchField ) );
    } else {
      return buildNode( batchField );
    }
  }
 
  private DrillNode buildNode( MaterializedField batchField ) {
    MinorType drillType = batchField.getType().getMinorType();
    if ( drillType == MinorType.MAP ) {
      return buildMapNode( batchField );
    } else if ( drillType == MinorType.LIST ) {
      return buildListNode( batchField );
    } else {
      return new ScalarNode( batchField );
    }
  }

  private DrillNode buildMapNode(MaterializedField batchField) {
    MapNode node = new MapNode( batchField );
    Collection<MaterializedField> children = batchField.getChildren();
    for ( MaterializedField child : children )
      node.children.add( makeField( child ) );
    return node;
  }

  private DrillNode buildListNode(MaterializedField batchField) {
    Collection<MaterializedField> children = batchField.getChildren();
    if ( children.size() != 1 )
      throw new IllegalStateException( "List field should have one child, found " + children.size() );
    return new ListNode( batchField, makeField( children.iterator().next() ) );
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