package org.apache.drill.jig.direct;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.Statement;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.json.builder.JsonBuilder;
import org.junit.Test;


// Vectors not matching up
// Field x marked as map, should be tuple
// For tuple field, add schema as accessor in FieldSchema
// Rename member to element in FieldSchema

public class TestDrillJson {
  
  public static class DrillProject
  {
    String projectName;
    String basePath;
    private DrillTestFile[] files;
    
    public DrillProject( String name, String path, DrillTestFile[] files ) {
      projectName = name;
      basePath = path;
      this.files = files;
    }

    public File getBaseDir(String drillPath) {
      return new File( drillPath, basePath );
    }
  }
  
  public static class DrillTestFile
  {
    String filePath;
    
    public DrillTestFile( String filePath ) {
      this.filePath = filePath;
    }

    public File getPath(File base) {
      return new File( base, filePath );
    }    
  }
  
  public static DrillProject drillProjects[] = buildTestFiles( );
  
  public static DrillProject[] buildTestFiles( ) {
    return new DrillProject[] {
//        new DrillProject( "drill-jdbc",
//            "exec/jdbc/src/test/resources",
//            new DrillTestFile[] {
//                new DrillTestFile( "donuts.json" ),
//          } ),
        new DrillProject( "drill-java-ecec",
            "exec/java-exec/src/test/resources/jsoninput",
            new DrillTestFile[] {
////                new DrillTestFile( "big_numeric.json" ), // Fails with out-of-range error (does not map to Decimal)
//                new DrillTestFile( "input2_modified.json" ),
                new DrillTestFile( "input2.json" ),
//                new DrillTestFile( "input3.json" ),
//                new DrillTestFile( "input4.json" ),
//                new DrillTestFile( "kvgen_complex_input.json" ),
//                new DrillTestFile( "large_int.json" ),
//                new DrillTestFile( "nestedArray.json" ),
//                new DrillTestFile( "nullableOrdered1.json" ),
//                new DrillTestFile( "repeated_list_bug.json" ),
         } )
        };
  }
  
  public static String drillPath = "../../drill/";

  @Test
  public void test() throws ExecutionSetupException, IOException, JigException {
    File drillDir = new File( drillPath );
//    System.out.println( drillDir.getCanonicalPath() );
    new DrillContextFactory( )
        .withEmbeddedDrillbit( )
        .build( )
        .getEmbeddedDrillbit()
        .defineWorkspace("dfs", "drill", drillDir.getCanonicalPath(), null);
    DirectConnection session = new DrillConnectionFactory( )
        .embedded( )
        .connect( );
    session.execute( "USE `dfs`.`drill`" );
    for ( DrillProject proj : drillProjects ) {
      File base = new File( proj.basePath );
      File projPath = proj.getBaseDir( drillPath );
      //System.out.println( base.getAbsolutePath() );
      assertTrue( projPath.exists() );
      for ( DrillTestFile file : proj.files ) {
        File testFile = file.getPath( projPath );
        assertTrue( testFile.exists() );
        testFile( session, new File( base, file.filePath ) );
      }
    }
    session.close();
  }

  private void testFile(DirectConnection session, File testFile) throws JigException, IOException {
    String stmt = "SELECT * FROM `" + testFile + "` LIMIT 20";
    Statement statement = session.prepare( stmt );
    ResultCollection results = statement.execute( );
    Map<String,String> props = new HashMap<>( );
    props.put( JsonGenerator.PRETTY_PRINTING, "true" );
    String baseName = testFile.getName();
    String outName = baseName.replaceAll( ".json", "-output.json" );
    File destDir = new File( "/tmp/out" );
    destDir.mkdirs();
    File destFile = new File( destDir, outName );
    FileWriter fileWriter = new FileWriter( destFile );
    JsonWriterFactory factory = Json.createWriterFactory( props );
    while ( results.next() ) {
      TupleSet tuples = results.tuples();
      while ( tuples.next() ) {
        JsonWriter writer = factory.createWriter(fileWriter);
        TupleValue tuple = tuples.tuple();
        JsonObject obj = JsonBuilder.build( tuple );
        writer.writeObject( obj );
      }
    }
    results.close( );
    fileWriter.close();
  }

}
