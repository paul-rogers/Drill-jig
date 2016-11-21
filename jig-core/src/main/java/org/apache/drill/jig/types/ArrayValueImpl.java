package org.apache.drill.jig.types;

import org.apache.drill.jig.accessor.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.impl.InternalArrayValue;
import org.apache.drill.jig.container.FieldValueContainer;
import org.apache.drill.jig.util.JigUtilities;

public class ArrayValueImpl implements InternalArrayValue {

  private final DataType memberType;
  private final boolean memberIsNullable;
  private final FieldValueContainer container;
  private ArrayAccessor arrayAccessor;

  public ArrayValueImpl( DataType memberType, boolean memberIsNullable, FieldValueContainer container ) {
    this.memberType = memberType;
    this.memberIsNullable = memberIsNullable;
    this.container = container;
  }

  @Override
  public DataType memberType() {
    return memberType;
  }

  @Override
  public boolean memberIsNullable() {
    return memberIsNullable;
  }

  public void bind( ArrayAccessor arrayAccessor ) {
    this.arrayAccessor = arrayAccessor;
  }

  @Override
  public int size() {
    if (arrayAccessor.isNull())
      return 0;
    return arrayAccessor.size();
  }

  @Override
  public FieldValue get(int i) {
    arrayAccessor.select( i );
    return container.get();
  }

  @Override
  public String toString( ) {
    StringBuilder buf = new StringBuilder( );
    visualize( buf, 0 );
    return buf.toString( );
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
//    buf.append( " size=" );
//    buf.append( size( ) );
    buf.append( ", member type=" );
    buf.append( memberType );
    buf.append( ", member nullable=" );
    buf.append( memberIsNullable );
    buf.append( "\n" );
    JigUtilities.visualizeLn(buf, indent + 1, "array accessor", arrayAccessor);
    JigUtilities.visualizeLn(buf, indent + 1, "container", container);
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "]" );
  }
}
