cd src/main/java
echo "start compiling"
time hadoop com.sun.tools.javac.Main ./*
echo "start packaging"
time jar cf project3.jar *.class
mv project3.jar ../../../project3.jar