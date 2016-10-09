package org.apache.drill.jig.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.ScalarValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.exception.JigException;

public class JigJdbcResultSet implements ResultSet
{
  private JigJdbcStatement stmt;
  private TupleSet tupleSet;
  private TupleValue tuple;
  private boolean wasNull;
  private JigResultSetMetaData resultMetaData;
  private boolean isEof;
  
  public JigJdbcResultSet( JigJdbcStatement stmt, TupleSet tupleSet ) {
    this.stmt = stmt;
    this.tupleSet = tupleSet;
    this.isEof = false;
  }
  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if ( iface.isAssignableFrom( TupleSet.class ) )
      return (T) tupleSet;
    if ( iface.isAssignableFrom( TupleValue.class ) )
      return (T) tuple;
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom( TupleValue.class ) ||
           iface.isAssignableFrom( TupleSet.class );
  }

  @Override
  public boolean next() throws SQLException {
    validateOpen( );
    try {
      isEof = ! tupleSet.next();
      tuple = isEof ? null : tupleSet.tuple();
      return ! isEof;
    } catch (JigException e) {
      throw wrapJigException( e );
    }
  }
  
  private void validateOpen( ) throws SQLException {
    if ( tupleSet == null )
      throw new SQLException( "ResultSet closed" );
  }

  private SQLException wrapJigException(JigException e) {
    return new JigWrapperException( e );
  }
  
  @Override
  public void close() throws SQLException {
    if ( tupleSet == null )
      return;
    tupleSet = null;
    tuple = null;
    resultMetaData = null;
    stmt.close( );
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    ScalarValue accessor = getScalar( columnIndex );
    if ( accessor == null )
      return null;
    // TODO: Type conversions
    try {
      return accessor.getString();
    }
    catch ( Exception e ) {
      return null;
    }
  }

  private ScalarValue getScalar(int columnIndex) throws SQLException {
    validateOpen( );
    wasNull = true;
    FieldValue field = tuple.field( columnIndex - 1 );
    if ( field == null )
      throw new SQLException( "Invalid column index: " + columnIndex );
    wasNull = field.isNull();
    if ( wasNull )
      return null;
    return field.asScalar();
  }
  
  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    ScalarValue accessor = getScalar( columnIndex );
    if ( accessor == null )
      return false;
    // TODO: Type conversions
    try {
      return accessor.getBoolean();
    }
    catch ( Exception e ) {
      return false;
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    ScalarValue accessor = getScalar( columnIndex );
    if ( accessor == null )
      return 0;
    // TODO: Type conversions
    try {
      return accessor.getInt();
    }
    catch ( Exception e ) {
      return 0;
    }
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    ScalarValue accessor = getScalar( columnIndex );
    if ( accessor == null )
      return 0;
    // TODO: Type conversions
    try {
      return accessor.getLong();
    }
    catch ( Exception e ) {
      return 0;
    }
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    ScalarValue accessor = getScalar( columnIndex );
    if ( accessor == null )
      return 0;
    // TODO: Type conversions
    try {
      return accessor.getFloat();
    }
    catch ( Exception e ) {
      return 0;
    }
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    ScalarValue accessor = getScalar( columnIndex );
    if ( accessor == null )
      return 0;
    // TODO: Type conversions
    try {
      return accessor.getDouble();
    }
    catch ( Exception e ) {
      return 0;
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // Ignore: Drill does not return warnings.
  }

  @Override
  public String getCursorName() throws SQLException {
    // Drill does not support cursor names.
    return null;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if ( tupleSet == null )
      return null;
    if ( resultMetaData == null )
      resultMetaData = new JigResultSetMetaData( tupleSet.schema( ) );
    return resultMetaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    validateOpen( );
    FieldSchema field = tupleSet.schema( ).field( columnLabel );
    if ( field == null )
      throw new SQLException( "Invalid column name: " + columnLabel );
    return field.index( ) + 1;
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    validateOpen( );
    return tupleSet.getIndex() < 0;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return isEof;
  }

  @Override
  public boolean isFirst() throws SQLException {
    validateOpen( );
    return tupleSet.getIndex() == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    throw scrollUnsupported( );
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw scrollUnsupported( );
  }
  
  private UnsupportedException scrollUnsupported( ) throws SQLException {
    validateOpen( );
    return new UnsupportedException( "scroll" );
  }

  @Override
  public void afterLast() throws SQLException {
    throw scrollUnsupported( );
  }

  @Override
  public boolean first() throws SQLException {
    throw scrollUnsupported( );
  }

  @Override
  public boolean last() throws SQLException {
    throw scrollUnsupported( );
  }

  @Override
  public int getRow() throws SQLException {
    return tupleSet.getIndex( ) + 1;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw scrollUnsupported( );
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw scrollUnsupported( );
  }

  @Override
  public boolean previous() throws SQLException {
    throw scrollUnsupported( );
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if ( direction != ResultSet.FETCH_FORWARD ) {
      throw scrollUnsupported( );
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    validateOpen( );
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    validateOpen( );
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void insertRow() throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateRow() throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void deleteRow() throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void refreshRow() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    validateOpen( );
    // Nothing to do.
  }

  @Override
  public Statement getStatement() throws SQLException {
    validateOpen( );
    return stmt;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return stmt.isClosed( );
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw streamsUnsupported( );
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw streamsUnsupported( );
  }

  private UnsupportedException streamsUnsupported() {
    return new UnsupportedException( "streams" );
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    updatesUnsupported( );
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    updatesUnsupported( );
  }

  private void updatesUnsupported() throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "updates" );
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

}
