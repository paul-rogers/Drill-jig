package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;

/**
 * The field accessor provides a uniform way to access the value,
 * nullness, and type of a field. Each attribute has a different accessor
 * to keep accessor implementations simple.
 */

public interface FieldAccessor {

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
  
  public interface VariantIndexedAccessor extends IndexedAccessor, TypeAccessor {
  }
  
  public interface ArrayAccessor extends IndexedAccessor
  {
    int size( );
    Object getArray( );
    FieldAccessor memberAccessor( );
  }
  
  public interface Resetable {
    void reset( );
  }

  boolean isNull();
}
