JFLAGS = -cp
JVM = java
JC = javac
DEST = .
.SUFFIXES: .java .class
CLASSPATH = .\:/usr/local/Thrift/*

CLASSES = \
          Node.java \
          Client.java \
          SuperNode.java

.java.class:
	$(JC) $(JFLAGS) $(CLASSPATH) $*.java -d $(DEST)

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class

sn: classes
	$(JVM) $(JFLAGS) $(CLASSPATH) SuperNode

n: classes
	$(JVM) $(JFLAGS) $(CLASSPATH) Node $(NUM)

cli: classes
	$(JVM) $(JFLAGS) $(CLASSPATH) Client

.PHONY: sn n cli