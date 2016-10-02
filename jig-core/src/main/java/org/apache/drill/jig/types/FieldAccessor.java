package org.apache.drill.jig.types;

import java.math.BigDecimal;

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

  boolean isNull();
}
