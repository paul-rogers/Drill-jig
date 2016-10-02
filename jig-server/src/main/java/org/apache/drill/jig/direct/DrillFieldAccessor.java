package org.apache.drill.jig.direct;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.ScalarValue;
import org.apache.drill.jig.exception.ValueConversionError;

public abstract class DrillFieldAccessor implements FieldValue
{
  public VectorRecordReader reader;
  public FieldSchema schema;
  public int fieldIndex;
  private ValueVector.Accessor genericAccessor;
  
  public void bind( VectorRecordReader reader, FieldSchema schema ) {
    this.reader = reader;
    this.schema = schema;
    this.fieldIndex = schema.index();
  }

  public void bindVector( ) {
    genericAccessor = getVector( ).getAccessor();
  }

  @Override
  public DataType type() {
    return schema.type();
  }

  @Override
  public Cardinality getCardinality() {
    return schema.getCardinality();
  }

  @Override
  public boolean isNull() {
    return genericAccessor.isNull( rowIndex( ) );
  }
  
  public ValueVector getVector( ) {
    return reader.getRecord().getVector( fieldIndex ).getValueVector();
  }
  
  protected int rowIndex( ) {
    return reader.getRecordIndex();
  }

  @Override
  public ScalarValue asScalar() {
    throw notSupportedError( "scalar" );
  }

  @Override
  public ArrayValue asArray() {
    throw notSupportedError( "array" );
  }

  @Override
  public AnyAccessor asAny() {
    throw notSupportedError( "any" );
  }

  public ValueConversionError notSupportedError(String type) {
    throw new ValueConversionError( "Cannot convert " + schema.getDisplayType( ) +
                " to " + type );
  }
  
  public abstract static class DrillScalarAccessor extends DrillFieldAccessor implements ScalarValue
  {
    @Override
    public ScalarValue asScalar() {
      return this;
    }

    @Override
    public boolean getBoolean() {
      throw notSupportedError( DataType.BOOLEAN.displayName() );
    }   

    @Override
    public byte getByte() {
      throw notSupportedError( DataType.INT8.displayName() );
    }

    @Override
    public int getInt() {
      throw notSupportedError( DataType.INT32.displayName( ) );
    }

    @Override
    public short getShort() {
      throw notSupportedError( DataType.INT16.displayName() );
    }

    @Override
    public long getLong() {
      throw notSupportedError( DataType.INT64.displayName() );
    }

    @Override
    public float getFloat() {
      throw notSupportedError( DataType.STRING.displayName() );
    }

    @Override
    public double getDouble() {
      throw notSupportedError( DataType.FLOAT64.displayName() );
    }

    @Override
    public BigDecimal getDecimal() {
      throw notSupportedError( DataType.DECIMAL.displayName() );
    }

    @Override
    public String getString() {
      throw notSupportedError( DataType.STRING.displayName() );
    }

    @Override
    public byte[] getBlob() {
      throw notSupportedError( DataType.BLOB.displayName() );
    }

    @Override
    public LocalDate getDate() {
      throw notSupportedError( DataType.DATE.displayName() );
    }

    @Override
    public LocalDateTime getDateTime() {
      throw notSupportedError( DataType.LOCAL_DATE_TIME.displayName() );
    }

    @Override
    public Period getUTCTime() {
      throw notSupportedError( DataType.UTC_DATE_TIME.displayName() );
    }

    @Override
    public Object getValue() {
      throw notSupportedError( "Object" );
    }
  }
  
  public static class NullAccessor extends DrillFieldAccessor
  {
    @Override
    public boolean isNull( ) {
      return true;
    }
  }
  
  public static class BitVectorAccessor extends DrillScalarAccessor
  {
    BitVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      BitVector v; 
      if ( schema.getCardinality() == Cardinality.OPTIONAL ) {
        v = ((NullableBitVector) getVector( )).getValuesVector();
      } else {
        v = (BitVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public boolean getBoolean()
    {
      return accessor.get( rowIndex( ) ) != 0;
    }

    @Override
    public Object getValue() {
      return getBoolean( );
    }
  }

  public static class IntVectorAccessor extends DrillScalarAccessor
  {
    IntVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      IntVector v; 
      if ( schema.getCardinality() == Cardinality.OPTIONAL ) {
        v = ((NullableIntVector) getVector( )).getValuesVector();
      } else {
        v = (IntVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public int getInt()
    {
      return accessor.get( rowIndex( ) );
    }

    @Override
    public Object getValue() {
      return getInt( );
    }
  }

  public static class BigIntVectorAccessor extends DrillScalarAccessor
  {
    BigIntVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      BigIntVector v; 
      if ( schema.getCardinality() == Cardinality.OPTIONAL ) {
        v = ((NullableBigIntVector) getVector( )).getValuesVector();
      } else {
        v = (BigIntVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public long getLong()
    {
      return accessor.get( rowIndex( ) );
    }

    @Override
    public Object getValue() {
      return getLong( );
    }
  }

  public static class Float4VectorAccessor extends DrillScalarAccessor
  {
    Float4Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      Float4Vector v; 
      if ( schema.getCardinality() == Cardinality.OPTIONAL ) {
        v = ((NullableFloat4Vector) getVector( )).getValuesVector();
      } else {
        v = (Float4Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public float getFloat()
    {
      return accessor.get( rowIndex( ) );
    }

    @Override
    public Object getValue() {
      return getFloat( );
    }
  }

  public static class Float8VectorAccessor extends DrillScalarAccessor
  {
    Float8Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      Float8Vector v; 
      if ( schema.getCardinality() == Cardinality.OPTIONAL ) {
        v = ((NullableFloat8Vector) getVector( )).getValuesVector();
      } else {
        v = (Float8Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public double getDouble()
    {
      return accessor.get( rowIndex( ) );
    }

    @Override
    public Object getValue() {
      return getDouble( );
    }
  }

  public static class VarCharVectorAccessor extends DrillScalarAccessor
  {
    VarCharVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      VarCharVector v; 
      if ( schema.getCardinality() == Cardinality.OPTIONAL ) {
        v = ((NullableVarCharVector) getVector( )).getValuesVector();
      } else {
        v = (VarCharVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public String getString()
    {
      return new String( accessor.get( rowIndex( ) ),
          org.apache.drill.jig.api.Constants.utf8Charset );
    }

    @Override
    public Object getValue() {
      return getString( );
    }
  }
}
