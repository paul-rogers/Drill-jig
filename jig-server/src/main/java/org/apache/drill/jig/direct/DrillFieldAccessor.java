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
import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldAccessor;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.ValueConversionError;

public abstract class DrillFieldAccessor implements FieldAccessor
{
  public VectorRecordReader reader;
  public FieldSchema schema;
  public int fieldIndex;
  private ValueVector.Accessor genericAccessor;
  
  public void bind( VectorRecordReader reader, FieldSchema schema ) {
    this.reader = reader;
    this.schema = schema;
    this.fieldIndex = schema.getIndex();
  }

  public void bindVector( ) {
    genericAccessor = getVector( ).getAccessor();
  }

  @Override
  public DataType getType() {
    return schema.getType();
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
  public ScalarAccessor asScalar() {
    throw notSupportedError( "scalar" );
  }

  @Override
  public ArrayAccessor asArray() {
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
  
  public abstract static class DrillScalarAccessor extends DrillFieldAccessor implements ScalarAccessor
  {
    @Override
    public ScalarAccessor asScalar() {
      return this;
    }

    @Override
    public boolean getBoolean() {
      throw notSupportedError( DataType.BOOLEAN.getDisplayName() );
    }   

    @Override
    public byte getByte() {
      throw notSupportedError( DataType.INT8.getDisplayName() );
    }

    @Override
    public int getInt() {
      throw notSupportedError( DataType.INT32.getDisplayName( ) );
    }

    @Override
    public short getShort() {
      throw notSupportedError( DataType.INT16.getDisplayName() );
    }

    @Override
    public long getLong() {
      throw notSupportedError( DataType.INT64.getDisplayName() );
    }

    @Override
    public float getFloat() {
      throw notSupportedError( DataType.STRING.getDisplayName() );
    }

    @Override
    public double getDouble() {
      throw notSupportedError( DataType.FLOAT64.getDisplayName() );
    }

    @Override
    public BigDecimal getDecimal() {
      throw notSupportedError( DataType.DECIMAL.getDisplayName() );
    }

    @Override
    public String getString() {
      throw notSupportedError( DataType.STRING.getDisplayName() );
    }

    @Override
    public byte[] getBlob() {
      throw notSupportedError( DataType.BLOB.getDisplayName() );
    }

    @Override
    public LocalDate getDate() {
      throw notSupportedError( DataType.DATE.getDisplayName() );
    }

    @Override
    public LocalDateTime getDateTime() {
      throw notSupportedError( DataType.LOCAL_DATE_TIME.getDisplayName() );
    }

    @Override
    public Period getUTCTime() {
      throw notSupportedError( DataType.UTC_DATE_TIME.getDisplayName() );
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
