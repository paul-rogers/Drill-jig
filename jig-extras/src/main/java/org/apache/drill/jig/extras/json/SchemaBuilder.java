package org.apache.drill.jig.extras.json;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.ArrayFieldSchemaImpl;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.extras.json.ObjectParser.ArrayNode;
import org.apache.drill.jig.extras.json.ObjectParser.FieldNode;
import org.apache.drill.jig.extras.json.ObjectParser.SchemaNode;
import org.apache.drill.jig.extras.json.ObjectParser.TupleNode;
import org.apache.drill.jig.types.FieldValueFactory;

public class SchemaBuilder {
  
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

  private TupleNode root;
  private boolean flatten;
  private FieldValueFactory factory;
  private TupleSchemaImpl schema;

  public SchemaBuilder( TupleNode node, FieldValueFactory factory ) {
    this.root = node;
    this.factory = factory;
  }
  
  public SchemaBuilder flatten( boolean flag ) {
    flatten = flag;
    return this;
  }
  
  public TupleSchema build( ) {
    schema = new TupleSchemaImpl( );
    Context rootContext = new Context( );
    buildTuple( root, rootContext );
    return schema;
  }

  private void buildTuple(TupleNode tuple, Context rootContext) {
    for ( SchemaNode node : tuple.children ) {
      if ( node instanceof FieldNode )
        buildField( (FieldNode) node, rootContext );
      else if ( node instanceof ArrayNode )
        buildArray( (ArrayNode) node, rootContext );
      else if ( node instanceof TupleNode ) {
        if ( flatten ) {
          flattenTuple ( (TupleNode) node, rootContext );
        } else {
          buildMap( (TupleNode) node, rootContext );
        }
      } else
        throw new IllegalStateException( "Unknown node type: " + node.getClass().getSimpleName() );
    }
  }

  private void buildField(FieldNode node, Context context) {
    schema.add( new FieldSchemaImpl( 
        context.fullName( node.name ), 
        node.type, 
        context.effectiveNullable( node.nullable ) ) );
  }

  private void buildArray(ArrayNode node, Context context) {
    schema.add( buildArraySchema( node, context ) );
  }

  private FieldSchemaImpl buildArraySchema(ArrayNode node, Context context) {
    SchemaNode member = node.member;
    FieldSchemaImpl memberSchema;
    if ( member instanceof FieldNode )
      memberSchema = new FieldSchemaImpl( "*",
          member.type,
          member.nullable );
    else if ( node instanceof ArrayNode )
      memberSchema = buildArraySchema( (ArrayNode) member, new Context( ) );
    else if ( member instanceof TupleNode ) {
      memberSchema = new FieldSchemaImpl( "*",
          DataType.MAP,
          member.nullable );
    } else
      throw new IllegalStateException( "Unknown node type: " + node.getClass().getSimpleName() );
    return new ArrayFieldSchemaImpl( 
        context.fullName( node.name ), 
        context.effectiveNullable( node.nullable ) ,
        memberSchema );
  }

  private void flattenTuple(TupleNode node, Context context) {
    Context nestedContext = context.nest( node.name, node.nullable );
    buildTuple( node, nestedContext );
  }

  private void buildMap(TupleNode node, Context context) {
    schema.add( new FieldSchemaImpl( 
        context.fullName( node.name ), 
        DataType.MAP, 
        context.effectiveNullable( node.nullable ) ) );
  }
}
