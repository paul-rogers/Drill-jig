package org.apache.drill.jig.direct;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.*;
import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.types.Int64Conversions;
import org.apache.drill.jig.util.JigUtilities;

//--------------------------------------------------------------
// WARNING: This code is generated!
// Modify src/main/codegen/templates/VectorAccessor.java
// Then regenerate this file by:
// $ cd src/main/codegen
// $ fmpp
//--------------------------------------------------------------

/**
 * Accessors for the "simple" Drill types: required, optional and repeated forms.
 * The code here is generated from the same meta-data used to generate Drill's
 * own vector classes.
 */

public abstract class VectorAccessor implements FieldAccessor {

  protected VectorRecordReader reader;
  protected boolean nullable;
  protected int fieldIndex;
  private ValueVector.Accessor genericAccessor;

  public void define( boolean nullable, int fieldIndex ) {
    this.nullable = nullable;
    this.fieldIndex = fieldIndex;
  }

  public void bindReader( VectorRecordReader reader ) {
    this.reader = reader;
  }

  public void bindVector( ) {
    genericAccessor = getVector( ).getAccessor();
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
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( " field index = " );
    buf.append( fieldIndex );
    buf.append( ", nullable = " );
    buf.append( nullable );
    buf.append( "]" );
  }

  /**
   * Base class for scalar (required or optional) vectors accessors.
   */

  public static class DrillScalarAccessor extends VectorAccessor {
  }

  /**
   * Base class for scalar to access individual elements within a
   * repeated vector.
   */

  public static class DrillElementAccessor extends VectorAccessor implements IndexedAccessor {

    protected int elementIndex;

    @Override
    public void bind( int index ) {
      elementIndex = index;
    }
  }


  /**
   * Jig field accessor for a Drill TinyInt vector (Nullable or Required)
   * returned as a Jig Int8 value encoded as a Java byte.
   */

  public static class TinyIntVectorAccessor extends DrillScalarAccessor implements Int8Accessor
  {
    TinyIntVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      TinyIntVector v;
      if ( nullable ) {
        v = ((NullableTinyIntVector) getVector( )).getValuesVector();
      } else {
        v = (TinyIntVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public byte getByte()
    {
      return accessor.get( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill TinyInt repeated vector
   * returned as a Jig Int8 value encoded as a Java byte.
   */

  public static class TinyIntElementAccessor extends DrillElementAccessor implements Int8Accessor
  {
    RepeatedTinyIntVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedTinyIntVector) getVector()).getAccessor( );
    }

    @Override
    public byte getByte()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill UInt1 vector (Nullable or Required)
   * returned as a Jig Int16 value encoded as a Java short.
   */

  public static class UInt1VectorAccessor extends DrillScalarAccessor implements Int16Accessor
  {
    UInt1Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      UInt1Vector v;
      if ( nullable ) {
        v = ((NullableUInt1Vector) getVector( )).getValuesVector();
      } else {
        v = (UInt1Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public short getShort()
    {
      return accessor.get( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill UInt1 repeated vector
   * returned as a Jig Int16 value encoded as a Java short.
   */

  public static class UInt1ElementAccessor extends DrillElementAccessor implements Int16Accessor
  {
    RepeatedUInt1Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedUInt1Vector) getVector()).getAccessor( );
    }

    @Override
    public short getShort()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill UInt2 vector (Nullable or Required)
   * returned as a Jig Int32 value encoded as a Java int.
   */

  public static class UInt2VectorAccessor extends DrillScalarAccessor implements Int32Accessor
  {
    UInt2Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      UInt2Vector v;
      if ( nullable ) {
        v = ((NullableUInt2Vector) getVector( )).getValuesVector();
      } else {
        v = (UInt2Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public int getInt()
    {
      return accessor.get( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill UInt2 repeated vector
   * returned as a Jig Int32 value encoded as a Java int.
   */

  public static class UInt2ElementAccessor extends DrillElementAccessor implements Int32Accessor
  {
    RepeatedUInt2Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedUInt2Vector) getVector()).getAccessor( );
    }

    @Override
    public int getInt()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill SmallInt vector (Nullable or Required)
   * returned as a Jig Int16 value encoded as a Java short.
   */

  public static class SmallIntVectorAccessor extends DrillScalarAccessor implements Int16Accessor
  {
    SmallIntVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      SmallIntVector v;
      if ( nullable ) {
        v = ((NullableSmallIntVector) getVector( )).getValuesVector();
      } else {
        v = (SmallIntVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public short getShort()
    {
      return accessor.get( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill SmallInt repeated vector
   * returned as a Jig Int16 value encoded as a Java short.
   */

  public static class SmallIntElementAccessor extends DrillElementAccessor implements Int16Accessor
  {
    RepeatedSmallIntVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedSmallIntVector) getVector()).getAccessor( );
    }

    @Override
    public short getShort()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Int vector (Nullable or Required)
   * returned as a Jig Int32 value encoded as a Java int.
   */

  public static class IntVectorAccessor extends DrillScalarAccessor implements Int32Accessor
  {
    IntVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      IntVector v;
      if ( nullable ) {
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
  }

  /**
   * Jig array element accessor for a Drill Int repeated vector
   * returned as a Jig Int32 value encoded as a Java int.
   */

  public static class IntElementAccessor extends DrillElementAccessor implements Int32Accessor
  {
    RepeatedIntVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedIntVector) getVector()).getAccessor( );
    }

    @Override
    public int getInt()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill UInt4 vector (Nullable or Required)
   * returned as a Jig Int64 value encoded as a Java long.
   */

  public static class UInt4VectorAccessor extends DrillScalarAccessor implements Int64Accessor
  {
    UInt4Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      UInt4Vector v;
      if ( nullable ) {
        v = ((NullableUInt4Vector) getVector( )).getValuesVector();
      } else {
        v = (UInt4Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public long getLong()
    {
      return accessor.get( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill UInt4 repeated vector
   * returned as a Jig Int64 value encoded as a Java long.
   */

  public static class UInt4ElementAccessor extends DrillElementAccessor implements Int64Accessor
  {
    RepeatedUInt4Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedUInt4Vector) getVector()).getAccessor( );
    }

    @Override
    public long getLong()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Float4 vector (Nullable or Required)
   * returned as a Jig Float32 value encoded as a Java float.
   */

  public static class Float4VectorAccessor extends DrillScalarAccessor implements Float32Accessor
  {
    Float4Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Float4Vector v;
      if ( nullable ) {
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
  }

  /**
   * Jig array element accessor for a Drill Float4 repeated vector
   * returned as a Jig Float32 value encoded as a Java float.
   */

  public static class Float4ElementAccessor extends DrillElementAccessor implements Float32Accessor
  {
    RepeatedFloat4Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedFloat4Vector) getVector()).getAccessor( );
    }

    @Override
    public float getFloat()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Decimal9 vector (Nullable or Required)
   * returned as a Jig Int32 value encoded as a Java int.
   */

  public static class Decimal9VectorAccessor extends DrillScalarAccessor implements Int32Accessor
  {
    Decimal9Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Decimal9Vector v;
      if ( nullable ) {
        v = ((NullableDecimal9Vector) getVector( )).getValuesVector();
      } else {
        v = (Decimal9Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public int getInt()
    {
      return accessor.get( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill Decimal9 repeated vector
   * returned as a Jig Int32 value encoded as a Java int.
   */

  public static class Decimal9ElementAccessor extends DrillElementAccessor implements Int32Accessor
  {
    RepeatedDecimal9Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedDecimal9Vector) getVector()).getAccessor( );
    }

    @Override
    public int getInt()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill BigInt vector (Nullable or Required)
   * returned as a Jig Int64 value encoded as a Java long.
   */

  public static class BigIntVectorAccessor extends DrillScalarAccessor implements Int64Accessor
  {
    BigIntVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      BigIntVector v;
      if ( nullable ) {
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
  }

  /**
   * Jig array element accessor for a Drill BigInt repeated vector
   * returned as a Jig Int64 value encoded as a Java long.
   */

  public static class BigIntElementAccessor extends DrillElementAccessor implements Int64Accessor
  {
    RepeatedBigIntVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedBigIntVector) getVector()).getAccessor( );
    }

    @Override
    public long getLong()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill UInt8 vector (Nullable or Required)
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class UInt8VectorAccessor extends DrillScalarAccessor implements DecimalAccessor
  {
    UInt8Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      UInt8Vector v;
      if ( nullable ) {
        v = ((NullableUInt8Vector) getVector( )).getValuesVector();
      } else {
        v = (UInt8Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return Int64Conversions.unsignedToDecimal( accessor.get( rowIndex( ) ) );
    }
  }

  /**
   * Jig array element accessor for a Drill UInt8 repeated vector
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class UInt8ElementAccessor extends DrillElementAccessor implements DecimalAccessor
  {
    RepeatedUInt8Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedUInt8Vector) getVector()).getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return Int64Conversions.unsignedToDecimal( accessor.get( rowIndex( ), elementIndex ) );
    }
  }

  /**
   * Jig field accessor for a Drill Float8 vector (Nullable or Required)
   * returned as a Jig Float64 value encoded as a Java double.
   */

  public static class Float8VectorAccessor extends DrillScalarAccessor implements Float64Accessor
  {
    Float8Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Float8Vector v;
      if ( nullable ) {
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
  }

  /**
   * Jig array element accessor for a Drill Float8 repeated vector
   * returned as a Jig Float64 value encoded as a Java double.
   */

  public static class Float8ElementAccessor extends DrillElementAccessor implements Float64Accessor
  {
    RepeatedFloat8Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedFloat8Vector) getVector()).getAccessor( );
    }

    @Override
    public double getDouble()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Decimal18 vector (Nullable or Required)
   * returned as a Jig Int64 value encoded as a Java long.
   */

  public static class Decimal18VectorAccessor extends DrillScalarAccessor implements Int64Accessor
  {
    Decimal18Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Decimal18Vector v;
      if ( nullable ) {
        v = ((NullableDecimal18Vector) getVector( )).getValuesVector();
      } else {
        v = (Decimal18Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public long getLong()
    {
      return accessor.get( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill Decimal18 repeated vector
   * returned as a Jig Int64 value encoded as a Java long.
   */

  public static class Decimal18ElementAccessor extends DrillElementAccessor implements Int64Accessor
  {
    RepeatedDecimal18Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedDecimal18Vector) getVector()).getAccessor( );
    }

    @Override
    public long getLong()
    {
      return accessor.get( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Decimal28Dense vector (Nullable or Required)
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class Decimal28DenseVectorAccessor extends DrillScalarAccessor implements DecimalAccessor
  {
    Decimal28DenseVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Decimal28DenseVector v;
      if ( nullable ) {
        v = ((NullableDecimal28DenseVector) getVector( )).getValuesVector();
      } else {
        v = (Decimal28DenseVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return accessor.getObject( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill Decimal28Dense repeated vector
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class Decimal28DenseElementAccessor extends DrillElementAccessor implements DecimalAccessor
  {
    RepeatedDecimal28DenseVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedDecimal28DenseVector) getVector()).getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return accessor.getSingleObject( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Decimal38Dense vector (Nullable or Required)
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class Decimal38DenseVectorAccessor extends DrillScalarAccessor implements DecimalAccessor
  {
    Decimal38DenseVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Decimal38DenseVector v;
      if ( nullable ) {
        v = ((NullableDecimal38DenseVector) getVector( )).getValuesVector();
      } else {
        v = (Decimal38DenseVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return accessor.getObject( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill Decimal38Dense repeated vector
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class Decimal38DenseElementAccessor extends DrillElementAccessor implements DecimalAccessor
  {
    RepeatedDecimal38DenseVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedDecimal38DenseVector) getVector()).getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return accessor.getSingleObject( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Decimal38Sparse vector (Nullable or Required)
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class Decimal38SparseVectorAccessor extends DrillScalarAccessor implements DecimalAccessor
  {
    Decimal38SparseVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Decimal38SparseVector v;
      if ( nullable ) {
        v = ((NullableDecimal38SparseVector) getVector( )).getValuesVector();
      } else {
        v = (Decimal38SparseVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return accessor.getObject( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill Decimal38Sparse repeated vector
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class Decimal38SparseElementAccessor extends DrillElementAccessor implements DecimalAccessor
  {
    RepeatedDecimal38SparseVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedDecimal38SparseVector) getVector()).getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return accessor.getSingleObject( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Decimal28Sparse vector (Nullable or Required)
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class Decimal28SparseVectorAccessor extends DrillScalarAccessor implements DecimalAccessor
  {
    Decimal28SparseVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Decimal28SparseVector v;
      if ( nullable ) {
        v = ((NullableDecimal28SparseVector) getVector( )).getValuesVector();
      } else {
        v = (Decimal28SparseVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return accessor.getObject( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill Decimal28Sparse repeated vector
   * returned as a Jig Decimal value encoded as a Java BigDecimal.
   */

  public static class Decimal28SparseElementAccessor extends DrillElementAccessor implements DecimalAccessor
  {
    RepeatedDecimal28SparseVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedDecimal28SparseVector) getVector()).getAccessor( );
    }

    @Override
    public BigDecimal getDecimal()
    {
      return accessor.getSingleObject( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill VarChar vector (Nullable or Required)
   * returned as a Jig String value encoded as a Java String.
   */

  public static class VarCharVectorAccessor extends DrillScalarAccessor implements StringAccessor
  {
    VarCharVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      VarCharVector v;
      if ( nullable ) {
        v = ((NullableVarCharVector) getVector( )).getValuesVector();
      } else {
        v = (VarCharVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public String getString()
    {
      return accessor.getObject( rowIndex( ) ).toString( );
    }
  }

  /**
   * Jig array element accessor for a Drill VarChar repeated vector
   * returned as a Jig String value encoded as a Java String.
   */

  public static class VarCharElementAccessor extends DrillElementAccessor implements StringAccessor
  {
    RepeatedVarCharVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedVarCharVector) getVector()).getAccessor( );
    }

    @Override
    public String getString()
    {
      return accessor.getSingleObject( rowIndex( ), elementIndex ).toString( );
    }
  }

  /**
   * Jig field accessor for a Drill Var16Char vector (Nullable or Required)
   * returned as a Jig String value encoded as a Java String.
   */

  public static class Var16CharVectorAccessor extends DrillScalarAccessor implements StringAccessor
  {
    Var16CharVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      Var16CharVector v;
      if ( nullable ) {
        v = ((NullableVar16CharVector) getVector( )).getValuesVector();
      } else {
        v = (Var16CharVector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public String getString()
    {
      return accessor.getObject( rowIndex( ) );
    }
  }

  /**
   * Jig array element accessor for a Drill Var16Char repeated vector
   * returned as a Jig String value encoded as a Java String.
   */

  public static class Var16CharElementAccessor extends DrillElementAccessor implements StringAccessor
  {
    RepeatedVar16CharVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedVar16CharVector) getVector()).getAccessor( );
    }

    @Override
    public String getString()
    {
      return accessor.getSingleObject( rowIndex( ), elementIndex );
    }
  }

  /**
   * Jig field accessor for a Drill Bit vector (Nullable or Required)
   * returned as a Jig Boolean value encoded as a Java boolean.
   */

  public static class BitVectorAccessor extends DrillScalarAccessor implements BooleanAccessor
  {
    BitVector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      BitVector v;
      if ( nullable ) {
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
  }

  /**
   * Jig array element accessor for a Drill Bit repeated vector
   * returned as a Jig Boolean value encoded as a Java boolean.
   */

  public static class BitElementAccessor extends DrillElementAccessor implements BooleanAccessor
  {
    RepeatedBitVector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedBitVector) getVector()).getAccessor( );
    }

    @Override
    public boolean getBoolean()
    {
      return accessor.get( rowIndex( ), elementIndex ) != 0;
    }
  }

  private static Class<? extends DrillScalarAccessor> scalarAccessors[ ] =
      buildScalarAccessorTable( );

  private static Class<? extends DrillScalarAccessor>[] buildScalarAccessorTable() {
    @SuppressWarnings("unchecked")
    Class<? extends DrillScalarAccessor> table[] =
        new Class[ MinorType.values().length ];
    table[MinorType.TINYINT.ordinal( )] = TinyIntVectorAccessor.class;
    table[MinorType.UINT1.ordinal( )] = UInt1VectorAccessor.class;
    table[MinorType.UINT2.ordinal( )] = UInt2VectorAccessor.class;
    table[MinorType.SMALLINT.ordinal( )] = SmallIntVectorAccessor.class;
    table[MinorType.INT.ordinal( )] = IntVectorAccessor.class;
    table[MinorType.UINT4.ordinal( )] = UInt4VectorAccessor.class;
    table[MinorType.FLOAT4.ordinal( )] = Float4VectorAccessor.class;
    table[MinorType.DECIMAL9.ordinal( )] = Decimal9VectorAccessor.class;
    table[MinorType.BIGINT.ordinal( )] = BigIntVectorAccessor.class;
    table[MinorType.UINT8.ordinal( )] = UInt8VectorAccessor.class;
    table[MinorType.FLOAT8.ordinal( )] = Float8VectorAccessor.class;
    table[MinorType.DECIMAL18.ordinal( )] = Decimal18VectorAccessor.class;
    table[MinorType.DECIMAL28DENSE.ordinal( )] = Decimal28DenseVectorAccessor.class;
    table[MinorType.DECIMAL38DENSE.ordinal( )] = Decimal38DenseVectorAccessor.class;
    table[MinorType.DECIMAL38SPARSE.ordinal( )] = Decimal38SparseVectorAccessor.class;
    table[MinorType.DECIMAL28SPARSE.ordinal( )] = Decimal28SparseVectorAccessor.class;
    table[MinorType.VARCHAR.ordinal( )] = VarCharVectorAccessor.class;
    table[MinorType.VAR16CHAR.ordinal( )] = Var16CharVectorAccessor.class;
    table[MinorType.BIT.ordinal( )] = BitVectorAccessor.class;
    return table;
  }

  private static Class<? extends DrillElementAccessor> elementAccessors[ ] =
      buildElementAccessorTable( );

  private static Class<? extends DrillElementAccessor>[] buildElementAccessorTable() {
    @SuppressWarnings("unchecked")
    Class<? extends DrillElementAccessor> table[] =
        new Class[ MinorType.values().length ];
    table[MinorType.TINYINT.ordinal( )] = TinyIntElementAccessor.class;
    table[MinorType.UINT1.ordinal( )] = UInt1ElementAccessor.class;
    table[MinorType.UINT2.ordinal( )] = UInt2ElementAccessor.class;
    table[MinorType.SMALLINT.ordinal( )] = SmallIntElementAccessor.class;
    table[MinorType.INT.ordinal( )] = IntElementAccessor.class;
    table[MinorType.UINT4.ordinal( )] = UInt4ElementAccessor.class;
    table[MinorType.FLOAT4.ordinal( )] = Float4ElementAccessor.class;
    table[MinorType.DECIMAL9.ordinal( )] = Decimal9ElementAccessor.class;
    table[MinorType.BIGINT.ordinal( )] = BigIntElementAccessor.class;
    table[MinorType.UINT8.ordinal( )] = UInt8ElementAccessor.class;
    table[MinorType.FLOAT8.ordinal( )] = Float8ElementAccessor.class;
    table[MinorType.DECIMAL18.ordinal( )] = Decimal18ElementAccessor.class;
    table[MinorType.DECIMAL28DENSE.ordinal( )] = Decimal28DenseElementAccessor.class;
    table[MinorType.DECIMAL38DENSE.ordinal( )] = Decimal38DenseElementAccessor.class;
    table[MinorType.DECIMAL38SPARSE.ordinal( )] = Decimal38SparseElementAccessor.class;
    table[MinorType.DECIMAL28SPARSE.ordinal( )] = Decimal28SparseElementAccessor.class;
    table[MinorType.VARCHAR.ordinal( )] = VarCharElementAccessor.class;
    table[MinorType.VAR16CHAR.ordinal( )] = Var16CharElementAccessor.class;
    table[MinorType.BIT.ordinal( )] = BitElementAccessor.class;
    return table;
  }

  public static DrillScalarAccessor getScalarAccessor( MinorType drillType ) {
    return instanceOf( scalarAccessors[ drillType.ordinal( ) ] );
  }

  public static DrillElementAccessor getElementAccessor( MinorType drillType ) {
    return instanceOf( elementAccessors[ drillType.ordinal( ) ] );
  }

  public static <T> T instanceOf(Class<? extends T> theClass) {
    if ( theClass == null )
      return null;
    try {
      return theClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException( "Could not create accessor", e );
    }
  }
}
