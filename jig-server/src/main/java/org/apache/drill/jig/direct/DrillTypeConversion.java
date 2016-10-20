package org.apache.drill.jig.direct;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.jig.api.DataType;

/**
 * @see <a href="https://docs.google.com/spreadsheets/d/1z_DIN4jajur1eKUqJ8ScjTZODnwc19cJcmz-i9VLYeU/edit#gid=733693293">
 * Data Type Summary</a>
 */

public class DrillTypeConversion
{
  public static class DrillDataType
  {
    public final MinorType drillType;
    public final DataType jigType;
    public final String sqlName;
    public final boolean drillSupported;
    public final boolean jigSupported;
    
    public DrillDataType( MinorType drillType, DataType jigType, String sqlType,
                          boolean drillSupported, boolean jigSupported ) {
      this.drillType = drillType;
      this.jigType = jigType;
      this.sqlName = sqlType;
      this.drillSupported = drillSupported;
      this.jigSupported = jigSupported;
    }
    
    public Class<? extends ValueVector> getVectorClass( DataMode mode ) {
      try {
        return BasicTypeHelper.getValueVectorClass( drillType, mode );
      }
      catch ( UnsupportedOperationException e ) {
        return null;
      }
    }
    
    public boolean isNullable( ) {
      return getVectorClass( DataMode.REQUIRED ) !=
             getVectorClass( DataMode.OPTIONAL );
    }
    
    public boolean isConversionSupported( ) {
      return drillSupported && jigSupported;
    }
  }
  
  public static final List<DrillDataType> drillTypes = makeDrillTypes( );
  public static final DrillDataType drillTypeIndex[] = makeDrillTypeIndex( drillTypes );
  
  public static List<DrillDataType> makeDrillTypes( ) {
    List<DrillDataType> types = new ArrayList<>( );
    
    // Integral types
    
    types.add( new DrillDataType(
        MinorType.BIT,
        DataType.BOOLEAN,
        "BOOLEAN",
        true, true ) );
    types.add( new DrillDataType(
        MinorType.TINYINT,
        DataType.INT8,
        "TINYINT",
        false, true ) ); // According to the spreadsheet
    types.add( new DrillDataType(
        MinorType.UINT1,
        DataType.INT16,
        "UINT1",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.SMALLINT,
        DataType.INT16,
        "SMALLINT",
        false, true ) ); // According to the spreadsheet
    types.add( new DrillDataType(
        MinorType.UINT2,
        DataType.INT32,
        "UINT2",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.INT,
        DataType.INT32,
        "INTEGER",
        true, true ) );
    types.add( new DrillDataType(
        MinorType.UINT4,
        DataType.INT64,
        "UINT4",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.BIGINT,
        DataType.INT64,
        "BIGINT",
        true, true ) );
    types.add( new DrillDataType(
        MinorType.UINT8,
        DataType.DECIMAL,
        "UINT8",
        true, false ) );
    
    // Floating point types
    
    types.add( new DrillDataType(
        MinorType.FLOAT4,
        DataType.FLOAT32,
        "FLOAT",
        true, true ) );
    types.add( new DrillDataType(
        MinorType.FLOAT8,
        DataType.FLOAT64,
        "DOUBLE",
        true, true ) );
    
    // Decimal types
    
    types.add( new DrillDataType(
        MinorType.DECIMAL9,
        DataType.DECIMAL,
        "DECIMAL",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.DECIMAL18,
        DataType.DECIMAL,
        "DECIMAL",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.DECIMAL28SPARSE,
        DataType.DECIMAL,
        "DECIMAL",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.DECIMAL28DENSE,
        DataType.DECIMAL,
        "DECIMAL",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.DECIMAL38SPARSE,
        DataType.DECIMAL,
        "DECIMAL",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.DECIMAL38DENSE,
        DataType.DECIMAL,
        "DECIMAL",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.MONEY,
        DataType.DECIMAL,
        "MONEY",
        false, false ) );
    
    // Date & Time
    
    types.add( new DrillDataType(
        MinorType.DATE,
        DataType.DATE,
        "DATE",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.TIME,
        DataType.UNDEFINED,
        "TIME",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.TIMETZ,
        DataType.UNDEFINED,
        "TIMETZ",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.TIMETZ,
        DataType.UNDEFINED,
        "TIMETZ",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.TIMESTAMP,
        DataType.UNDEFINED,
        "TIMESTAMP",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.TIMESTAMPTZ,
        DataType.UNDEFINED,
        "TIMESTAMPTZ",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.INTERVAL,
        DataType.UNDEFINED,
        "INTERVAL",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.INTERVALYEAR,
        DataType.UNDEFINED,
        "INTERVALYEAR",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.INTERVALDAY,
        DataType.UNDEFINED,
        "INTERVALDAY",
        true, false ) );   
    
    // Strings
    
    types.add( new DrillDataType(
        MinorType.FIXEDCHAR,
        DataType.STRING,
        "CHAR",
        false, true ) );
    types.add( new DrillDataType(
        MinorType.VARCHAR,
        DataType.STRING,
        "VARCHAR",
        true, true ) );
    types.add( new DrillDataType(
        MinorType.FIXED16CHAR,
        DataType.STRING,
        "CHAR16",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.VAR16CHAR,
        DataType.STRING,
        "VARCHAR16",
        false, false ) );
    
    // Binary
    
    types.add( new DrillDataType(
        MinorType.FIXEDBINARY,
        DataType.BLOB,
        "BLOB",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.VARBINARY,
        DataType.BLOB,
        "BLOB",
        true, false ) );
    
    // Structured types
    
    types.add( new DrillDataType(
        MinorType.NULL,
        DataType.NULL,
        "NULL",
        true, true ) );
    types.add( new DrillDataType(
        MinorType.MAP,
        DataType.MAP,
        "MAP",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.LIST,
        DataType.LIST,
        "LIST",
        true, false ) );
    types.add( new DrillDataType(
        MinorType.GENERIC_OBJECT,
        DataType.UNDEFINED,
        "OBJECT",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.UNION,
        DataType.UNDEFINED,
        "UNION",
        false, false ) );
    types.add( new DrillDataType(
        MinorType.LATE,
        DataType.UNDEFINED,
        "LATE",
        true, false ) );

    
    return types;
  }
  
  private static DrillDataType[] makeDrillTypeIndex(List<DrillDataType> drillTypes) {
    DrillDataType[] types = new DrillDataType[ MinorType.values().length ];
    for ( DrillDataType drillType : drillTypes ) {
      types[ drillType.drillType.ordinal() ] = drillType;
    }
    return types;
  }
  
  public static DrillDataType lookupType( MinorType type ) {
    return drillTypeIndex[ type.ordinal() ];
  }
  
  public static DataType drillToJigType( MinorType drillType ) {
    int i = drillType.ordinal();
    if ( i >= jigDataTypeForDrillType.length ) {
      return DataType.UNDEFINED;
    }
    return jigDataTypeForDrillType[i];
  }
  
  public static DataType jigDataTypeForDrillType[] = makeDrillToJigTable( );
  
  private static DataType[] makeDrillToJigTable() {
    DataType table[ ] = new DataType[38];
    table[ MinorType.LATE.ordinal()] = DataType.UNDEFINED; // x
    table[ MinorType.MAP.ordinal()] = DataType.TUPLE; // x
    table[ MinorType.TINYINT.ordinal()] = DataType.INT8; // x
    table[ MinorType.SMALLINT.ordinal()] = DataType.INT16; // x
    table[ MinorType.INT.ordinal()] = DataType.INT32; // x
    table[ MinorType.BIGINT.ordinal()] = DataType.INT64; // x
    table[ MinorType.DECIMAL9.ordinal()] = DataType.FLOAT32; // x
    table[ MinorType.DECIMAL18.ordinal()] = DataType.FLOAT64; // x
    table[ MinorType.DECIMAL28SPARSE.ordinal()] = DataType.DECIMAL; // x
    table[ MinorType.DECIMAL38SPARSE.ordinal()] = DataType.DECIMAL; // x
    table[ MinorType.MONEY.ordinal()] = DataType.DECIMAL; // x
    table[ MinorType.DATE.ordinal()] = DataType.DATE; // x
    table[ MinorType.TIME.ordinal()] = DataType.LOCAL_DATE_TIME; // x
    table[ MinorType.TIMETZ.ordinal()] = DataType.UTC_DATE_TIME; // x
    table[ MinorType.TIMESTAMPTZ.ordinal()] = DataType.UTC_DATE_TIME; // x
    table[ MinorType.TIMESTAMP.ordinal()] = DataType.UTC_DATE_TIME; // x
    table[ MinorType.INTERVAL.ordinal()] = DataType.DATE_TIME_SPAN; // x
    table[ MinorType.FLOAT4.ordinal()] = DataType.FLOAT32; // x
    table[ MinorType.FLOAT8.ordinal()] = DataType.FLOAT64; // x
    table[ MinorType.BIT.ordinal()] = DataType.BOOLEAN; // x
    table[ MinorType.FIXEDCHAR.ordinal()] = DataType.STRING; // x
    table[ MinorType.FIXED16CHAR.ordinal()] = DataType.STRING; // x
    table[ MinorType.FIXEDBINARY.ordinal()] = DataType.BLOB; // x
    table[ MinorType.VARCHAR.ordinal()] = DataType.STRING; // x
    table[ MinorType.VAR16CHAR.ordinal()] = DataType.STRING; // x
    table[ MinorType.VARBINARY.ordinal()] = DataType.BLOB; // x
    table[ MinorType.UINT1.ordinal()] = DataType.INT16; // x
    table[ MinorType.UINT2.ordinal()] = DataType.INT32; // x
    table[ MinorType.UINT4.ordinal()] = DataType.INT64; // x
    table[ MinorType.UINT8.ordinal()] = DataType.DECIMAL; // x
    table[ MinorType.DECIMAL28DENSE.ordinal()] = DataType.DECIMAL; // x
    table[ MinorType.DECIMAL38DENSE.ordinal()] = DataType.DECIMAL; // x
    table[ MinorType.NULL.ordinal()] = DataType.NULL; // x
    table[ MinorType.INTERVALYEAR.ordinal()] = DataType.UNDEFINED; // x
    table[ MinorType.INTERVALDAY.ordinal()] = DataType.UNDEFINED; // x
    table[ MinorType.LIST.ordinal()] = DataType.LIST; // x
    table[ MinorType.GENERIC_OBJECT.ordinal()] = DataType.UNDEFINED; // x
    table[ MinorType.UNION.ordinal()] = DataType.UNDEFINED; // x
    return table;
  }
  
  public static String javaTypeForJigType[] = makeJavaTypeTable( );

  private static String[] makeJavaTypeTable() {
    String types[] = new String[ DataType.values().length ];
    
    types[ DataType.UNDEFINED.ordinal( ) ] = null;
    types[ DataType.NULL.ordinal( ) ] = null;
    types[ DataType.BOOLEAN.ordinal( ) ] = "boolean"; 
    types[ DataType.INT8.ordinal( ) ] = "byte";
    types[ DataType.INT16.ordinal( ) ] = "short";
    types[ DataType.INT32.ordinal( ) ] = "int";
    types[ DataType.INT64.ordinal( ) ] = "long"; 
    types[ DataType.FLOAT32.ordinal( ) ] = "float";
    types[ DataType.FLOAT64.ordinal( ) ] = "double";
    types[ DataType.DECIMAL.ordinal( ) ] = "BigDecimal";
    types[ DataType.STRING.ordinal( ) ] = "String";
    types[ DataType.BLOB.ordinal( ) ] = "byte[]"; 
    types[ DataType.DATE.ordinal( ) ] = "LocalDate";
    types[ DataType.LOCAL_DATE_TIME.ordinal( ) ] = "LocalDateTime"; 
    types[ DataType.UTC_DATE_TIME.ordinal( ) ] = "DateTime"; 
    types[ DataType.DATE_TIME_SPAN.ordinal( ) ] = "Period";
    types[ DataType.LIST.ordinal( ) ] = null;
    types[ DataType.MAP.ordinal( ) ] = null;
    return types;
  }

  public static DrillDataType getDrillType(MinorType minorType) {
    return drillTypeIndex[ minorType.ordinal() ];
  }

}
