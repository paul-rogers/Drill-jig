package org.apache.drill.jig.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;

public class JigResultSetMetaData implements ResultSetMetaData
{
  private TupleSchema schema;

  public JigResultSetMetaData(TupleSchema schema) {
    this.schema = schema;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if ( iface.isAssignableFrom( TupleSchema.class ) )
      return (T) schema;
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom( TupleSchema.class );
  }

  @Override
  public int getColumnCount() throws SQLException {
    return schema.count();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    // No Drill field is auto-increment
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    // No Drill field is case sensitive
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    // All Drill fields are searchable, if only by table scan
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    if ( getJigField( column ).getCardinality( ) == Cardinality.OPTIONAL )
      return columnNullable;
    else
      return columnNoNulls;
  }

  private FieldSchema getJigField(int column) {
    FieldSchema field = schema.field( column - 1 );
    if ( field == null )
      throw new ArrayIndexOutOfBoundsException( "Column index " + column );
    return field;
  }

  private static boolean signed[] = initSigned( );
  
  private static boolean[] initSigned() {
    boolean signed[] = new boolean[ DataType.values().length ];
    signed[ DataType.DECIMAL.ordinal() ] = true;
    signed[ DataType.FLOAT32.ordinal() ] = true;
    signed[ DataType.FLOAT64.ordinal() ] = true;
    signed[ DataType.INT8.ordinal() ] = true;
    signed[ DataType.INT16.ordinal() ] = true;
    signed[ DataType.INT32.ordinal() ] = true;
    signed[ DataType.INT64.ordinal() ] = true;
    signed[ DataType.DATE_TIME_SPAN.ordinal() ] = true;
    return signed;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    FieldSchema field = getJigField( column );
    return signed[ field.type().ordinal() ];
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getColumnName( column );
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getJigField( column ).name();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  private static int sqlTypes[] = initTypes( );

  private static int[] initTypes() {
    int types[] = new int[ DataType.values().length ];
    types[ DataType.VARIANT.ordinal() ] = Types.JAVA_OBJECT;
    types[ DataType.BLOB.ordinal() ] = Types.BLOB;
    types[ DataType.BOOLEAN.ordinal() ] = Types.BOOLEAN;
    types[ DataType.DATE.ordinal() ] = Types.DATE;
    types[ DataType.DATE_TIME_SPAN.ordinal() ] = 0; // No SQL Equiv
    types[ DataType.DECIMAL.ordinal() ] = Types.DECIMAL;
    types[ DataType.FLOAT32.ordinal() ] = Types.FLOAT;
    types[ DataType.FLOAT64.ordinal() ] = Types.DOUBLE;
    types[ DataType.INT8.ordinal() ] = Types.TINYINT;
    types[ DataType.INT16.ordinal() ] = Types.SMALLINT;
    types[ DataType.INT32.ordinal() ] = Types.INTEGER;
    types[ DataType.INT64.ordinal() ] = Types.BIGINT;
    types[ DataType.LIST.ordinal() ] = Types.ARRAY;
    types[ DataType.LOCAL_DATE_TIME.ordinal() ] = Types.DATE;
    types[ DataType.MAP.ordinal() ] = Types.STRUCT;
    types[ DataType.NULL.ordinal() ] = Types.NULL;
    types[ DataType.STRING.ordinal() ] = Types.VARCHAR;
    types[ DataType.UNDEFINED.ordinal() ] = 0;
    types[ DataType.UTC_DATE_TIME.ordinal() ] = Types.TIMESTAMP;
    return types;
  }
  
  @Override
  public int getColumnType(int column) throws SQLException {
    FieldSchema field = getJigField( column );
    return sqlTypes[ field.type().ordinal() ];
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return getJigField( column ).type().displayName();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    // Drill results are never writable.
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    // Drill results are never writable.
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    // Drill results are never writable.
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

}
