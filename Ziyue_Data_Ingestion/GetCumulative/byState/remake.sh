#!/bin/bash

rm *.class
javac -classpath `hadoop classpath` *.java
jar cvf DataIngestionCumulative.jar *.class
