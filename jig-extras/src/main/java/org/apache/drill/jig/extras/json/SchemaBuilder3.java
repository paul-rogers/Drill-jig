package org.apache.drill.jig.extras.json;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.impl.ArrayFieldSchemaImpl;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.extras.json.ObjectParser.*;
import org.apache.drill.jig.extras.json.JsonAccessor.*;
import org.apache.drill.jig.types.DataDef;
import org.apache.drill.jig.types.DataDef.*;
import org.apache.drill.jig.types.FieldAccessor;
import org.apache.drill.jig.types.FieldValueContainer;
import org.apache.drill.jig.types.FieldValueContainerSet;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.NullAccessor;

/**
 * Builds a set of accessors to translate a JSON object of a particular schema
 * into the Jig API format.
 * <ul>
 * <li>A JSON object represents the tuple.<li>
 * <li>The member of the tuple object represent fields. Fields are accessed
 * by key in the tuple objects. Fields may be either non-existent or a JSON
 * NULL value; both are treated as a null value (and the field schema must
 * be nullable.)</li>
 * <li>A simple JSON value is represented as a Jig scalar.</li>
 * <li>A JSON array is represented as a Jig List.</li>
 * <li>Non-root tuples can be represented either as a Jig map, or flattened
 * <li>into the root tuple. In this case, a name of form "a.b" means the
 * value a in the root tuple object (which must be a JSON object), then
 * the value b in the nested object.</li>
 * </ul>
 */

public class SchemaBuilder3 {

  public static class Context {
    String prefix;
    boolean nullable;
    
    public String fullName( String leafName ) {
      if ( prefix == null )
        return leafName;
      return prefix + "." + leafName;
    }
    
    public boolean effectiveNullable( boolean leafNullable ) {
      return nullable | leafNullable;
    }

    public Context nest(String childName, boolean childNullable) {
      Context nestedContext = new Context( );
      nestedContext.prefix = fullName( childName );
      nestedContext.nullable = effectiveNullable( childNullable );
      return nestedContext;
    }
  }
  
  public abstract class SchemaNode {
    
    public FieldSchema fieldSchema;
    public FieldAccessor accessor;
    public DataDef defn;
    
    public abstract void buildSchema( TupleSchemaImpl schema, Context context );
    protected abstract FieldSchemaImpl defineField( Context context );
    public abstract void buildAccessor( ObjectAccessor input );
    public abstract void buildDef( );
    
    public void collectFields(DataDef[] fieldDefs) {
      fieldDefs[fieldSchema.index()] = defn;
    }
    
    protected void addField( TupleSchemaImpl schema, FieldSchemaImpl field ) {
      schema.add( field );
      fieldSchema = field;
    }
  }
  
  public abstract class ObjectNode extends SchemaNode {
    public JsonObjectNode jsonNode;
    public List<SchemaNode> children = new ArrayList<>( );
    
    public ObjectNode(JsonObjectNode jsonRoot) {
      jsonNode = jsonRoot;
    }

    @Override
    public void buildDef( ) {
      for ( SchemaNode node : children ) {
        node.buildDef( );
      }
    }
    
    public void collectFields(DataDef[] fieldDefs) {
      for ( SchemaNode node : children ) {
        node.collectFields( fieldDefs );
      }
    }
  }
  
  public class TupleNode extends ObjectNode {
    
    public TupleNode(JsonObjectNode node) {
      super( node );
    }

    @Override
    public void buildSchema(TupleSchemaImpl schema, Context context) {
      context = new Context( );
      for ( SchemaNode node : children ) {
        node.buildSchema( schema, context );
      }      
    }

    @Override
    protected FieldSchemaImpl defineField(Context context) {
      throw new IllegalStateException( "Can't build a field for the root tuple" );
    }

    @Override
    public void buildAccessor( ObjectAccessor input ) {
      ObjectAccessor tupleAccessor = new TupleObjectAccessor( );
      accessor = tupleAccessor;
      for ( SchemaNode node : children ) {
        node.buildAccessor( tupleAccessor );
      }
    }
  }
  
  public class FlattenedObjectNode extends ObjectNode {
    
    public FlattenedObjectNode(JsonObjectNode node ) {
      super( node );
    }

    @Override
    public void buildSchema(TupleSchemaImpl schema, Context context) {
      Context thisContext = context.nest( jsonNode.name, jsonNode.nullable );
      for ( SchemaNode node : children ) {
        node.buildSchema( schema, thisContext );
      }
    }

    @Override
    protected FieldSchemaImpl defineField(Context context) {
      throw new IllegalStateException( "Can't build a field for a flattened tuple" );
    }

    @Override
    public void buildAccessor(ObjectAccessor input) {
      ObjectAccessor hiddenAccessor = new JsonObjectMemberAccessor( input, jsonNode.name );
      accessor = hiddenAccessor;
      for ( SchemaNode node : children ) {
        node.buildAccessor( hiddenAccessor );
      }
    }

    @Override
    public void collectFields(DataDef[] fieldDefs) {
      for ( SchemaNode node : children ) {
        node.collectFields( fieldDefs );
      }
    }
  }
  
  public class MapNode extends SchemaNode {
   
    public JsonObjectNode jsonNode;
   
    public MapNode(JsonObjectNode node) {
      jsonNode = node;
    }

    @Override
    protected FieldSchemaImpl defineField(Context context) {
      return new FieldSchemaImpl( 
          context.fullName( jsonNode.name ), 
          DataType.MAP, 
          context.effectiveNullable( jsonNode.nullable ) );
    }
    
    @Override
    public void buildSchema(TupleSchemaImpl schema, Context context) {
      addField( schema, defineField( context ) );
    }

    @Override
    public void buildAccessor(ObjectAccessor input) {
      accessor = new JsonMapAccessor( input, factory );
    }

    @Override
    public void buildDef( ) {
      defn = new MapDef( fieldSchema.nullable(), (MapValueAccessor) accessor );
    }
  }
  
  public class ArrayNode extends SchemaNode {

    public JsonArrayNode jsonNode;
    private SchemaNode member;
    
    public ArrayNode(JsonArrayNode node, SchemaNode member) {
      jsonNode = node;
      this.member = member;
    }

    @Override
    public void buildSchema(TupleSchemaImpl schema, Context context) {
      addField( schema, defineField( context ) );
    }

    @Override
    protected FieldSchemaImpl defineField(Context context) {
      member.fieldSchema = member.defineField( new Context( ) );
      return new ArrayFieldSchemaImpl( 
          context.fullName( jsonNode.name ), 
          context.effectiveNullable( jsonNode.nullable ),
          member.fieldSchema );
    }

    @Override
    public void buildAccessor(ObjectAccessor input) {
      JsonArrayAccessor arrayAccessor = new JsonArrayAccessor( input );
      accessor = arrayAccessor;
      member.buildAccessor( (ObjectAccessor) arrayAccessor.memberAccessor() );     
    }

    @Override
    public void buildDef( ) {
      member.buildDef( );
      defn = new ListDef( fieldSchema.nullable(), member.defn, (ArrayAccessor) accessor );
    }
  }
  
  public class ScalarNode extends SchemaNode {

    public JsonScalarNode jsonNode;
    
    public ScalarNode(JsonScalarNode node) {
      jsonNode = node;
    }

    @Override
    public void buildSchema(TupleSchemaImpl schema, Context context) {
      addField( schema, defineField( context ) );
    }

    @Override
    protected FieldSchemaImpl defineField(Context context) {
      return new FieldSchemaImpl( 
          context.fullName( jsonNode.name ), 
          jsonNode.type, 
          context.effectiveNullable( jsonNode.nullable ) );
    }

    @Override
    public void buildAccessor(ObjectAccessor input) {
      if ( jsonNode.type.isUndefined() )
        accessor = new NullAccessor( );
      else
        accessor = new JsonValueAccessor( input );
    }

    @Override
    public void buildDef( ) {
      defn = new ScalarDef( fieldSchema.type(), fieldSchema.nullable(), accessor );
    }
  }

  private boolean flatten;
  TupleNode tuple;
  private JsonObjectNode jsonRoot;
  FieldValueFactory factory = new FieldValueFactory( );
  private FieldValueContainerSet fieldValues;
  
  public SchemaBuilder3( JsonObjectNode root ) {
    this.jsonRoot = root;
  }
  
  public void flatten( boolean flag ) {
    flatten = flag;
  }
  
  public TupleSchemaImpl build( ) {
    tuple = buildTuple( );
    TupleSchemaImpl schema = new TupleSchemaImpl( );
    tuple.buildSchema( schema, null );
    tuple.buildAccessor( null );
    tuple.buildDef( );
    DataDef fieldDefs[] = new DataDef[schema.count()];
    tuple.collectFields( fieldDefs );
    fieldValues = buildFieldValues( fieldDefs );
    return schema;
  }
  
  public FieldValueContainerSet fieldValues( ) {
    return fieldValues;
  }
  
  protected TupleNode buildTuple( ) {
    TupleNode tuple = new TupleNode( jsonRoot );
    buildObjectFields( jsonRoot, tuple );
    return tuple;
  }

  private void buildObjectFields( JsonObjectNode object, ObjectNode parent )
  {
    for ( JsonSchemaNode node : object.children ) {
      parent.children.add( buildSchemaNode( node, flatten ) );
    }
  }

  private SchemaNode buildSchemaNode(JsonSchemaNode node, boolean flatten) {
    switch ( node.nodeType() ) {
    case ARRAY:
      JsonArrayNode arrayNode = (JsonArrayNode) node;
      ArrayNode array = new ArrayNode( arrayNode, buildSchemaNode( arrayNode.member, false ) );
      return array;

    case OBJECT:
      if ( flatten ) {
        FlattenedObjectNode fob = new FlattenedObjectNode( (JsonObjectNode) node );
        buildObjectFields( (JsonObjectNode) node, fob );
        return fob;
      } else {
        return new MapNode( (JsonObjectNode) node );
      }

    case SCALAR:
      return new ScalarNode( (JsonScalarNode) node );

    default:
      throw new IllegalArgumentException( "Unknown node type: " + node.nodeType( ) );
    }
  }
 
  public FieldValueContainerSet buildFieldValues( DataDef fieldDefs[] ) {
    int fieldCount = fieldDefs.length;
    FieldValueContainer values[] = new FieldValueContainer[ fieldCount ];
    for ( int i = 0;  i < fieldCount;  i++ ) {
      fieldDefs[i].build( factory );
      values[i] = fieldDefs[i].container;
    }
    return new FieldValueContainerSet( values );
  }

  public TupleObjectAccessor rootAccessor() {
    return (TupleObjectAccessor) tuple.accessor;
  }
  
}
