package org.apache.drill.jig.types;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.exception.ValueConversionError;

public class FieldValueFactory {

  public AbstractFieldValue buildValue(DataType type) {
    switch (type) {
    case BOOLEAN:
      return new BooleanFieldValue();
    case DECIMAL:
      return new DecimalFieldValue();
    case FLOAT32:
      return new Float32FieldValue();
    case FLOAT64:
      return new Float64FieldValue();
    case INT16:
      return new Int16FieldValue();
    case INT32:
      return new Int32FieldValue();
    case INT64:
      return new Int64FieldValue();
    case INT8:
      return new Int8FieldValue();
    case NULL:
      return new NullFieldValue();
    case STRING:
      return new StringFieldValue();
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case UTC_DATE_TIME:
      throw new IllegalStateException(
          "No field value yet implemented for type: " + type);
    case VARIANT:
    case NUMBER:
    case UNDEFINED:
      throw new IllegalStateException("No field value for type: " + type);
    default:
      return extendedValue(type);
    }
  }

  protected AbstractFieldValue extendedValue(DataType type) {
    throw new IllegalStateException("No field value for type: " + type);
  }

  public DataType objectToJigType(Object value) {
    if (value == null)
      return DataType.NULL;
    return classToJigType( value.getClass() );
  }
  
  public DataType classNameToJigType( String className ) {
    try {
      return classToJigType( getClass( ).getClassLoader().loadClass( className ) );
    } catch (ClassNotFoundException e) {
      throw new ValueConversionError( e.getMessage(), e );
    }
  }
  
  public DataType classToJigType( Class<? extends Object> valueClass ) {
   if ( Boolean.class.equals( valueClass ) )
      return DataType.BOOLEAN;
    else if ( Byte.class.equals( valueClass ))
      return DataType.INT8;
    else if ( Short.class.equals( valueClass ))
      return DataType.INT16;
    else if ( Integer.class.equals( valueClass ))
      return DataType.INT32;
    else if ( Long.class.equals( valueClass ))
      return DataType.INT64;
    else if ( Float.class.equals( valueClass ))
      return DataType.FLOAT32;
    else if ( Double.class.equals( valueClass ))
      return DataType.FLOAT64;
    else if ( BigDecimal.class.equals( valueClass ))
      return DataType.DECIMAL;
    else if ( String.class.equals( valueClass ))
      return DataType.STRING;
    else if ( Map.class.isAssignableFrom( valueClass ))
      return DataType.MAP;
    else if ( List.class.isAssignableFrom( valueClass ))
      return DataType.LIST;
    return extendedConversion(valueClass);
  }

  protected DataType extendedConversion(Class<? extends Object> valueClass) {
    throw new IllegalArgumentException("No Jig type for object of class "
        + valueClass.getSimpleName());
  }

  public DataType mergeTypes(DataType type1, DataType type2) {
    if (type1 == null || type1 == DataType.NULL)
      return type2;
    if (type2 == null || type2 == DataType.NULL)
      return type1;
    if (type1 == type2)
      return type1;
    if (type1.isScalar() && type2.isScalar())
      return DataType.VARIANT;
    throw new ValueConversionError("Incompatible types: " + type1 + " and "
        + type2);
  }
}
