#!/bin/bash

rm *.class
javac -classpath `hadoop classpath` *.java
jar cvf DataIngestion.jar *.class
