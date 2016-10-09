Dependencies include Hadoop, which requires that the Java tools.jar file be registered in Maven.

1. Find the JDK directory that contains tools.jar.
2. cd to that directory.
3. Enter:

`mvn install:install-file -DgroupId=jdk.tools -DartifactId=jdk.tools -Dpackaging=jar \`
`-Dversion=1.8 -Dfile=tools.jar -DgeneratePom=true`
