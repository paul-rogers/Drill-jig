package org.apache.drill.jig.accessor;

import java.util.List;

import org.apache.drill.jig.accessor.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ValueObjectAccessor;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.util.JigUtilities;

public class JavaListAccessor implements ArrayAccessor, ValueObjectAccessor {

  public class JavaListMemberAccessor implements IndexedAccessor, ObjectAccessor {

    private int index;

    @Override
    public void bind( int index ) {
      this.index = index;
    }

    @Override
    public boolean isNull() {
      return getObject( ) == null;
    }

    @Override
    public Object getObject() {
      return getList( ).get( index );
    }

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      buf.append( "]" );
    }
  }

  private ObjectAccessor listAccessor;
  private final JavaListMemberAccessor elementAccessor = new JavaListMemberAccessor( );
  private final FieldAccessor elementValueAccessor;

  public JavaListAccessor( ObjectAccessor listAccessor ) {
    this.listAccessor = listAccessor;
    elementValueAccessor = new BoxedAccessor( elementAccessor );
  }

  public JavaListAccessor( ObjectAccessor listAccessor, FieldValueFactory factory ) {
    this( factory );
    this.listAccessor = listAccessor;
  }

  public JavaListAccessor( FieldValueFactory factory ) {
    elementValueAccessor = factory.newVariantObjectAccessor( elementAccessor );
  }

  public void bind( ObjectAccessor accessor ) {
    listAccessor = accessor;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int size() {
    List list = getList( );
    if ( list == null )
      return 0;
    return list.size( );
  }

  @Override
  public Object getValue() {
    return listAccessor.getObject();
  }

  @Override
  public boolean isNull() {
    return listAccessor.isNull();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private List<? extends Object> getList( ) {
    if ( listAccessor.isNull( ) )
      return null;
    return (List) listAccessor.getObject( );
  }

  @Override
  public FieldAccessor elementAccessor( ) {
    return elementValueAccessor;
  }

  @Override
  public void select( int index ) {
    elementAccessor.bind( index );
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "list accessor = " );
    listAccessor.visualize( buf, indent + 2 );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "element accessor = " );
    elementAccessor.visualize( buf, indent + 2 );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "element value accessor = " );
    elementValueAccessor.visualize( buf, indent + 2 );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent );
    buf.append( "]" );
  }
}
