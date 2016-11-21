package org.apache.drill.jig.direct;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.*;
import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.types.Int64Conversions;
import org.apache.drill.jig.util.JigUtilities;

//--------------------------------------------------------------
// WARNING: This code is generated!
<#-- OK, so not this file, but the one that this file generates... -->
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

<#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#if drillToJig[drillType]?? >
      <#assign jigInfo=drillToJig[drillType]>
      <#assign jigType=jigInfo.jig!drillType>
      <#assign returnType=minor.javaType!jigInfo.returnType!type.javaType>
      <#assign getLabel=jigInfo.get!returnType?capitalize>
      <#assign notyet=jigInfo.notyet!false>
      <#assign asObject=jigInfo.asObject!false>
    <#else>
      <#assign jigType=drillType>
      <#assign returnType=minor.javaType!type.javaType>
      <#assign getLabel=returnType?capitalize>
      <#assign notyet=false>
      <#assign asObject=false>
    </#if>
    <#if ! notyet>

  /**
   * Jig field accessor for a Drill ${drillType} vector (Nullable or Required)
   * returned as a Jig ${jigType} value encoded as a Java ${returnType}.
   */

  public static class ${drillType}VectorAccessor extends DrillScalarAccessor implements ${jigType}Accessor
  {
    ${drillType}Vector.Accessor accessor;

    @Override
    @SuppressWarnings("resource")
    public void bindVector( ) {
      super.bindVector( );
      ${drillType}Vector v;
      if ( nullable ) {
        v = ((Nullable${drillType}Vector) getVector( )).getValuesVector();
      } else {
        v = (${drillType}Vector) getVector( );
      }
      accessor = v.getAccessor( );
    }

    @Override
    public ${returnType} get${getLabel}()
    {
    <#if drillType="Bit">
      return accessor.get( rowIndex( ) ) != 0;
    <#elseif drillType="UInt8">
      return Int64Conversions.unsignedToDecimal( accessor.get( rowIndex( ) ) );
    <#elseif drillType="VarChar">
      return accessor.getObject( rowIndex( ) ).toString( );
    <#elseif asObject>
      return accessor.getObject( rowIndex( ) );
    <#else>
      return accessor.get( rowIndex( ) );
    </#if>
    }
  }

  /**
   * Jig array element accessor for a Drill ${drillType} repeated vector
   * returned as a Jig ${jigType} value encoded as a Java ${returnType}.
   */

  public static class ${drillType}ElementAccessor extends DrillElementAccessor implements ${jigType}Accessor
  {
    Repeated${drillType}Vector.Accessor accessor;

    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((Repeated${drillType}Vector) getVector()).getAccessor( );
    }

    @Override
    public ${returnType} get${getLabel}()
    {
    <#if drillType="Bit">
      return accessor.get( rowIndex( ), elementIndex ) != 0;
    <#elseif drillType="UInt8">
      return Int64Conversions.unsignedToDecimal( accessor.get( rowIndex( ), elementIndex ) );
    <#elseif drillType="VarChar">
      return accessor.getSingleObject( rowIndex( ), elementIndex ).toString( );
    <#elseif asObject>
      return accessor.getSingleObject( rowIndex( ), elementIndex );
    <#else>
      return accessor.get( rowIndex( ), elementIndex );
    </#if>
    }
  }
    </#if>
  </#list>
</#list>

  private static Class<? extends DrillScalarAccessor> scalarAccessors[ ] =
      buildScalarAccessorTable( );

  private static Class<? extends DrillScalarAccessor>[] buildScalarAccessorTable() {
    @SuppressWarnings("unchecked")
    Class<? extends DrillScalarAccessor> table[] =
        new Class[ MinorType.values().length ];
<#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#if drillToJig[drillType]?? >
      <#assign jigInfo=drillToJig[drillType]>
      <#assign notyet=jigInfo.notyet!false>
    <#else>
      <#assign notyet=false>
    </#if>
    <#if ! notyet>
    table[MinorType.${drillType?upper_case}.ordinal( )] = ${drillType}VectorAccessor.class;
    </#if>
  </#list>
</#list>
    return table;
  }

  private static Class<? extends DrillElementAccessor> elementAccessors[ ] =
      buildElementAccessorTable( );

  private static Class<? extends DrillElementAccessor>[] buildElementAccessorTable() {
    @SuppressWarnings("unchecked")
    Class<? extends DrillElementAccessor> table[] =
        new Class[ MinorType.values().length ];
    <#list vv.types as type>
    <#list type.minor as minor>
      <#assign drillType=minor.class>
      <#if drillToJig[drillType]?? >
        <#assign jigInfo=drillToJig[drillType]>
        <#assign notyet=jigInfo.notyet!false>
      <#else>
        <#assign notyet=false>
      </#if>
      <#if ! notyet>
    table[MinorType.${drillType?upper_case}.ordinal( )] = ${drillType}ElementAccessor.class;
      </#if>
    </#list>
  </#list>
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
