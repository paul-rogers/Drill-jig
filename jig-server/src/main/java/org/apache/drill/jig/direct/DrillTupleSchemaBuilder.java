package org.apache.drill.jig.direct;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.direct.DrillTupleSchema.DrillFieldSchema;
import org.apache.drill.jig.direct.DrillTypeConversion.DrillDataType;

public class DrillTupleSchemaBuilder
{
  private BatchSchema batchSchema;

  public DrillTupleSchemaBuilder(BatchSchema schema) {
    batchSchema = schema;
  }

  public DrillTupleSchema build() {
    DrillTupleSchema schema = new DrillTupleSchema( );
    int fieldCount = batchSchema.getFieldCount();
    for ( int i = 0;  i < fieldCount;  i++ ) {
      MaterializedField batchField = batchSchema.getColumn( i );
      String name = batchField.getName();
      MinorType drillType =  batchField.getType().getMinorType();
      DataType type = mapDrillType( drillType );
      Cardinality cardinality = mapDrillCardinality( batchField.getDataMode() );
      FieldSchemaImpl field = new DrillFieldSchema( name, type, cardinality, drillType );
      schema.add( field );
    }
    schema.accessors = buildAccesors( schema );
    return schema;
  }

  private DataType mapDrillType(MinorType minorType) {
    DrillDataType drillType = DrillTypeConversion.getDrillType( minorType );
    if ( ! drillType.isConversionSupported() )
      return DataType.UNDEFINED;
    return drillType.jigType;
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
  }

  private Cardinality mapDrillCardinality(DataMode dataMode) {
    switch( dataMode ) {
    case OPTIONAL:
      return Cardinality.OPTIONAL;
    case REPEATED:
      return Cardinality.REPEATED;
    case REQUIRED:
      return Cardinality.REQUIRED;
    default:
      throw new IllegalStateException( "Unexpected DataMode: " + dataMode.name() );
    }
  }
  
  private DrillFieldAccessor[] buildAccesors(DrillTupleSchema schema) {
    int fieldCount = schema.count();
    DrillFieldAccessor accessors[] = new DrillFieldAccessor[ fieldCount ];
    for ( int i = 0;  i < fieldCount;  i++ ) {
      accessors[i] = buildAccessor( schema.getDrillField( i ) );
    }
    return accessors;
  }
  
  private static Class<? extends DrillFieldAccessor> accessors[ ] =
      buildAccessorTable( );

  private static Class<? extends DrillFieldAccessor>[] buildAccessorTable() {
    @SuppressWarnings("unchecked")
    Class<? extends DrillFieldAccessor> table[] =
        new Class[ MinorType.values().length ];
    table[MinorType.BIT.ordinal( )] = DrillFieldAccessor.BitVectorAccessor.class;
    table[MinorType.INT.ordinal( )] = DrillFieldAccessor.IntVectorAccessor.class;
    table[MinorType.BIGINT.ordinal( )] = DrillFieldAccessor.BigIntVectorAccessor.class;
    table[MinorType.FLOAT4.ordinal( )] = DrillFieldAccessor.Float4VectorAccessor.class;
    table[MinorType.FLOAT8.ordinal( )] = DrillFieldAccessor.Float8VectorAccessor.class;
    table[MinorType.VARCHAR.ordinal( )] = DrillFieldAccessor.VarCharVectorAccessor.class;
    return table;
  }

  // TODO: Create a table, and do selection based on Drill types,
  // not API types.
  
  private DrillFieldAccessor buildAccessor(DrillFieldSchema field) {
    Class<? extends DrillFieldAccessor> accessorClass =
        accessors[ field.drillType.ordinal( ) ];
    if ( accessorClass == null )
      accessorClass = DrillFieldAccessor.NullAccessor.class;
    try {
      return accessorClass.newInstance();
    } catch (InstantiationException e) {
      throw new IllegalStateException( e );
    } catch (IllegalAccessException e) {
      throw new IllegalStateException( e );
    }
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
  }

}