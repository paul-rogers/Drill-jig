package org.apache.drill.jig.extras.json.schema;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue.ValueType;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;

import org.apache.drill.jig.extras.json.reader.BufferingTupleReader;
import org.apache.drill.jig.extras.json.reader.JsonArrayReader;
import org.apache.drill.jig.extras.json.reader.JsonRecordReader;
import org.apache.drill.jig.extras.json.reader.JsonScannerException;
import org.apache.drill.jig.extras.json.reader.JsonTupleReader;
import org.apache.drill.jig.extras.json.reader.NullRecordReader;
import org.apache.drill.jig.extras.json.schema.JsonSchema.ArraySchema;
import org.apache.drill.jig.extras.json.schema.JsonSchema.JsonType;
import org.apache.drill.jig.extras.json.schema.JsonSchema.NumberSchema;
import org.apache.drill.jig.extras.json.schema.JsonSchema.ObjectSchema;
import org.apache.drill.jig.extras.json.schema.JsonSchema.StringSchema;
import org.apache.drill.jig.extras.json.schema.JsonSchema.ValueSchema;
import org.apache.drill.jig.extras.json.source.ObjectParser;
import org.apache.drill.jig.extras.json.source.ObjectParser.JsonArrayNode;
import org.apache.drill.jig.extras.json.source.ObjectParser.JsonObjectNode;
import org.apache.drill.jig.extras.json.source.ObjectParser.JsonSchemaNode;
import org.apache.drill.jig.types.FieldValueFactory;
import org.glassfish.json.CustomJsonReader;

public class InferSchema {

  private static final int DEFAULT_SAMPLE_SIZE = 100;
  private static OutputStreamWriter writer;

  public static void main(String[] args) {
    if ( args.length < 1 ) {
      System.err.println( "USAGE: InferSchema file.json [output]" );
      System.exit( -1 );
    }
    InferSchema tool = new InferSchema( );
    try {
      tool.infer( new File( args[0] ) );
    } catch (FileNotFoundException e) {
      System.err.println( "Could not read file: " + args[0] );
      System.err.println( e.getMessage() );
      System.exit( -1 );
    }
    if ( args.length < 2 ) {
      writer = new OutputStreamWriter( System.out );
      tool.writeSchema( writer );
      try {
        writer.flush();
      } catch (IOException e) {
        // Should never happen
      }
    } else {
      try ( Writer writer = new FileWriter( args[1] ) )
      {
        tool.writeSchema( writer );
      } catch (IOException e) {
        System.err.println( "Could not write file: " + args[1] );
        System.err.println( e.getMessage() );
        System.exit( -1 );
      }
    }
  }

  private int sampleSize = DEFAULT_SAMPLE_SIZE;
  private JsonType rootType;
  private JsonObjectNode inputSchema;
  private JsonSchema schema;

  public void infer(File inFile) throws FileNotFoundException {
    Reader reader = new FileReader( inFile );
    infer( reader );
  }
  
  public void infer( String json ) {
    infer( new StringReader( json ) );
  }
  
  private void infer( Reader reader ) {
    rootType = null;
    try ( BufferingTupleReader jsonReader = prepareReader( reader ) ) {
      inferSchema( jsonReader );
      buildJsonSchema( );
    }
  }

  @SuppressWarnings("resource")
  private BufferingTupleReader prepareReader( Reader in ) {
    BufferingTupleReader objectReader;
    JsonReader reader = new CustomJsonReader( in );
    JsonStructure struct;
    struct = reader.read();
    if ( struct == null ) {
      objectReader = new BufferingTupleReader( new NullRecordReader( ) );
      reader.close( );
      rootType = JsonType.NULL;
    }
    else if ( struct.getValueType() == ValueType.ARRAY ) {
      objectReader = new BufferingTupleReader( 
          new JsonArrayReader( (JsonArray) struct ) );
      reader.close( );
      rootType = JsonType.ARRAY;
    }
    else if ( struct.getValueType() == ValueType.OBJECT ) {
      objectReader = new BufferingTupleReader( new JsonRecordReader( reader ) );
      objectReader.push( (JsonObject) struct );
      rootType = JsonType.OBJECT;
    }
    else {
      throw new JsonScannerException( "Found unexpected JSON type " +
          struct.getValueType( ).toString() +
          " for first tuple" );
    }
    return objectReader;
  }
  
  public void inferSchema( JsonTupleReader recordReader ) {
    FieldValueFactory factory = new FieldValueFactory( );
    ObjectParser parser = new ObjectParser( factory );
    for ( int i = 0;  i < sampleSize;  i++ ) {
      JsonObject obj = recordReader.next();
      if ( obj == null ) {
        if ( i == 0 ) {
          rootType = JsonType.NULL;
          return;
        }
        break;
      }
      parser.addObject( obj );
    }
    inputSchema = parser.getJsonSchema( );
  }
  
  public void buildJsonSchema( ) {
    schema = new JsonSchema( );
    ValueSchema root;
    if ( rootType == JsonType.NULL ) {
      root = new ValueSchema( );
      root.type = rootType;
    } else if ( rootType == JsonType.OBJECT ) {
      root = buildValue( inputSchema );
    } else if ( rootType == JsonType.ARRAY ) {
      root = new ValueSchema( );
      root.type = rootType;
      ArraySchema array = new ArraySchema( );
      array.elementSchema = buildValue( inputSchema );
      root.typeSchema = array;
    } else {
      throw new IllegalStateException( "Unexpected type: " + rootType );
    }
    schema.root = root;
  }

  private ValueSchema buildValue(JsonSchemaNode node) {
    if ( node == null )
      return null;
    ValueSchema schema = new ValueSchema( );
    schema.name = node.name;
    schema.nullable = node.nullable;
    schema.hasNullable = true;
    schema.required = true;
    switch ( node.nodeType() ) {
    case ARRAY: {
      schema.type = JsonType.ARRAY;
      ArraySchema arraySchema = new ArraySchema( );
      arraySchema.elementSchema = buildValue( ((JsonArrayNode) node).element );
      schema.typeSchema = arraySchema;
      break;
    }
    case OBJECT: {
      schema.type = JsonType.OBJECT;
      ObjectSchema objSchema = new ObjectSchema( );
      for ( JsonSchemaNode child : ((JsonObjectNode) node).children ) {
        objSchema.addProperty( buildValue( child ) );
      }
      schema.typeSchema = objSchema;
      break;
    }
    case SCALAR: {
      switch ( node.type ) {
      case BOOLEAN:
        schema.type = JsonType.BOOLEAN;
        break;
      case NULL:
        schema.type = JsonType.NULL;
        break;
      case INT64:
        schema.type = JsonType.INTEGER;
        schema.typeSchema = new NumberSchema( );
        break;
      case NUMBER:
        schema.type = JsonType.NUMBER;
        schema.typeSchema = new NumberSchema( );
        break;
      case STRING:
        schema.type = JsonType.STRING;
        schema.typeSchema = new StringSchema( );
        break;
      case VARIANT:
        schema.type = JsonType.VARIANT;
        break;
      default:
        break;      
      }
      schema.jigType = node.type;
      break;
    }
    default:
      break;    
    }
    return schema;
  }
  
  public void writeSchema( Writer writer ) {
    JsonObject jsonSchema = schema.toJson( );
    Map<String,String> props = new HashMap<>( );
    props.put( JsonGenerator.PRETTY_PRINTING, "true" );
    JsonWriterFactory factory = Json.createWriterFactory( props );
    JsonWriter jsonWriter = factory.createWriter(writer);
    jsonWriter.writeObject( jsonSchema );
  }
}
