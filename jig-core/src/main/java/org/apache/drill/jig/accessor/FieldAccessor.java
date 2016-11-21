package org.apache.drill.jig.accessor;

import java.math.BigDecimal;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.util.Visualizable;

/**
 * The field accessor provides a uniform way to access the value,
 * nullness, and type of a field. Each attribute has a different accessor
 * to keep accessor implementations simple.
 */

public interface FieldAccessor extends Visualizable {

  public interface BooleanAccessor extends FieldAccessor {
    boolean getBoolean();
  }

  public interface Int8Accessor extends FieldAccessor {
    byte getByte();
  }

  public interface Int16Accessor extends FieldAccessor {
    short getShort();
  }

  public interface Int32Accessor extends FieldAccessor {
    int getInt();
  }

  public interface Int64Accessor extends FieldAccessor {
    long getLong();
  }

  public interface Float32Accessor extends FieldAccessor {
    float getFloat();
  }

  public interface Float64Accessor extends FieldAccessor {
    double getDouble();
  }

  public interface DecimalAccessor extends FieldAccessor {
    BigDecimal getDecimal();
  }

  public interface StringAccessor extends FieldAccessor {
    String getString();
  }

  public interface ObjectAccessor extends FieldAccessor {
    Object getObject();
  }

  public interface TypeAccessor extends FieldAccessor {
    DataType getType( );
  }

  public interface IndexedAccessor extends FieldAccessor {
    void bind( int index );
  }

  public interface ValueObjectAccessor extends FieldAccessor {

    /**
     * Return a Java representation of the value. The value is optional and
     * is implementation-specific.
     *
     * @return
     */

    Object getValue( );
  }

  /**
   * Interface to any array that can be presented as a
   * {@link ArrayValue}.
   */

  public interface ArrayAccessor extends FieldAccessor
  {
    /**
     * Returns the member accessor which must be invariant over the life of
     * this accessor, whether bound to an array or not. That is, the
     * type of accessor must be static. If the value is a structured type,
     * this value will be bound to the implementation of that value.
     * <p>
     * The value accessed by the member accessor is only defined when
     * this accessor is defined, and must be selected by calling
     * {@link #select(int)}.
     *
     * @return
     */

    FieldAccessor elementAccessor( );

    /**
     * Returns the size of array backing this accessor.
     * @return
     */

    int size( );

     /**
     * Select the array value backing the member accessor. Allows the member
     * accessor to be static, only the index of the selected value changes
     * over time.
     *
     * @param index
     */

    void select( int index );
  }

  public interface ArrayValueAccessor extends FieldAccessor
  {
    ArrayValue getArray( );
  }

  public interface MapValueAccessor extends FieldAccessor
  {
    MapValue getMap( );
  }

  public interface TupleValueAccessor extends FieldAccessor
  {
    TupleValue getTuple( );
  }

  public interface Resetable {
    void reset( );
  }

  boolean isNull();
}
