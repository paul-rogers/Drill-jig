package org.apache.drill.jig.extras.json;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import javax.json.JsonReader;
import javax.json.JsonStructure;

import org.glassfish.json.CustomJsonReader;


public class Tester
{

  public static void main(String[] args) {
//    private static final ObjectMapper MAPPER = new ObjectMapper()
//        .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
//        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    FileInputStream in;
    try {
      File empFile = new File( "/Users/progers/play/foodmart/employee.json" );
      in = new FileInputStream( empFile );
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return;
    }
//    JsonParser parser = MAPPER.getFactory().createParser(in);
////  JsonReader reader = Json.createReader( in );
    try ( JsonReader reader = new CustomJsonReader( in ) )
    {
      JsonStructure struct;
      while ( (struct = reader.read() ) != null ) {
        System.out.println( struct.toString( ) );
      }
    }
//  File empFile = new File( "/Users/progers/play/foodmart/employee.json" );
//  Gson gson = new GsonBuilder().create();
//    JsonStreamParser parser = new JsonStreamParser(new FileReader(empFile));
//    while(parser.hasNext())
//    {
//      System.out.println(gson.fromJson(parser.next(), Thing.class));
//    }  
    }

}
