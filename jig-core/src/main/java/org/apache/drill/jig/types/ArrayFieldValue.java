package org.apache.drill.jig.types;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ArrayValueAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.accessor.JavaListAccessor;
import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.impl.InternalArrayValue;
import org.apache.drill.jig.container.FieldValueContainer;
import org.apache.drill.jig.container.VariantFieldValueContainer;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.util.JigUtilities;

public class ArrayFieldValue extends AbstractStructuredValue {

  public static class SimpleArrayValueAccessor implements ArrayValueAccessor {

    private ArrayAccessor arrayAccessor;
    private InternalArrayValue arrayValue;

    public SimpleArrayValueAccessor( ArrayAccessor arrayAccessor, InternalArrayValue arrayValue ) {
      this.arrayAccessor = arrayAccessor;
      this.arrayValue = arrayValue;
    }

    @Override
    public boolean isNull() {
      return arrayAccessor.isNull( );
    }

    @Override
    public ArrayValue getArray() {
      return arrayValue;
    }

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      buf.append( "\n" );
      JigUtilities.visualizeLn(buf, indent + 1, "array accessor", arrayAccessor);
      JigUtilities.visualizeLn(buf, indent + 1, "array value", arrayValue);
      JigUtilities.indent( buf, indent );
      buf.append( "]" );
    }
  }

  public static class JavaListFieldValue extends ArrayFieldValue
  {
    JavaListAccessor listAccessor;

    public JavaListFieldValue( FieldValueFactory factory ) {
      listAccessor = new JavaListAccessor( factory );
      FieldValueContainer container = new VariantFieldValueContainer( factory );
      container.bind( listAccessor.elementAccessor() );
      ArrayValueImpl arrayValue = new ArrayValueImpl( DataType.VARIANT, true, container );
      arrayValue.bind( listAccessor );
      accessor = new SimpleArrayValueAccessor( listAccessor, arrayValue );
    }

    @Override
    public void bind(FieldAccessor accessor) {
      listAccessor.bind( (ObjectAccessor) accessor );
    }

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      buf.append( "\n" );
      JigUtilities.visualize(buf, indent + 1, "list accessor", listAccessor);
      JigUtilities.indent(buf, indent + 1);
      buf.append( "]" );
    }
  }

  protected ArrayValueAccessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (ArrayValueAccessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.LIST;
  }

  @Override
  public boolean isNull() {
    return accessor.isNull();
  }

  @Override
  public MapValue getMap() {
    throw new ValueConversionError( "Cannot convert a list to a map" );
  }

  @Override
  public TupleValue getTuple() {
    throw new ValueConversionError( "Cannot convert a list to a tuple" );
  }

  @Override
  public ArrayValue getArray() {
    return accessor.getArray( );
  }

  @Override
  public Object getValue() {
    throw new ValueConversionError( "Cannot convert a list to an object" );
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.visualizeLn(buf, indent + 1, "array value accessor", accessor);
    JigUtilities.indent(buf, indent + 1);
    buf.append( "]" );
  }
}
