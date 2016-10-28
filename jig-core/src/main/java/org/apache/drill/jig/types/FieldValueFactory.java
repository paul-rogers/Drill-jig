package org.apache.drill.jig.types;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.drill.jig.accessor.BoxedAccessor.VariantBoxedAccessor;
import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.exception.ValueConversionError;

/**
 * Builds a field value given a data type, and converts Java object
 * types to Jig types. Designed to allow the type system to be
 * extensible. Scalar types are simple and are provided here.
 * Map and list types are implementation-specific and can be
 * created by implementation-specific subclasses of this class. 
 */

public class FieldValueFactory {

  /**
   * Create a field value given a Jig data type. The caller must
   * bind the field value to an accessor that provides the actual
   * value.
   * 
   * @param type
   * @return a new field value
   */
  
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
    case UNDEFINED:
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
      throw new IllegalStateException("No field value for type: " + type);
    default:
      return extendedValue(type);
    }
  }

  protected AbstractFieldValue extendedValue(DataType type) {
    throw new IllegalStateException("No field value for type: " + type);
  }

  /**
   * Convert a Java object to the corresponding Jig type.
   * 
   * @param value
   * @return
   */
  
  public DataType objectToJigType(Object value) {
    if (value == null)
      return DataType.NULL;
    return classToJigType( value.getClass() );
  }
  
  /**
   * Convert a Java class name to the corresponding Jig type.
   * 
   * @param className
   * @return
   */
  
  public DataType classNameToJigType( String className ) {
    try {
      return classToJigType( getClass( ).getClassLoader().loadClass( className ) );
    } catch (ClassNotFoundException e) {
      throw new ValueConversionError( e.getMessage(), e );
    }
  }
  
  /**
   * Convert a Java class to the corresponding Jig type.
   * 
   * @param valueClass
   * @return
   */
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

  /**
   * Given two Jig types, compute the common type. Null and any time is
   * the other type. Like types are merged as that type. Two dislike
   * scalars merge to a VARIANT. All other merges are illegal.
   * 
   * @param type1
   * @param type2
   * @return
   */

  public DataType mergeTypes(DataType type1, DataType type2) {
    if (type1 == null || type1 == DataType.NULL || type1 == DataType.UNDEFINED)
      return type2;
    if (type2 == null || type2 == DataType.NULL || type2 == DataType.UNDEFINED)
      return type1;
    if (type1 == type2)
      return type1;
    if (type1.isScalar() && type2.isScalar())
      return DataType.VARIANT;
    throw new ValueConversionError("Incompatible types: " + type1 + " and "
        + type2);
  }
  
  public FieldAccessor newVariantObjectAccessor( ObjectAccessor objAccessor ) {
    return new VariantBoxedAccessor( objAccessor, this );
  }
}
