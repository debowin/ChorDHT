JFLAGS = -cp
JFLAGS_RUN = -Xmx1g -Xss8192k
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
	$(JVM) $(JFLAGS_RUN) $(JFLAGS) $(CLASSPATH) SuperNode

n: classes
	$(JVM) $(JFLAGS_RUN) $(JFLAGS) $(CLASSPATH) Node $(NUM)

cli: classes
	$(JVM) $(JFLAGS_RUN) $(JFLAGS) $(CLASSPATH) Client $(FILE)

.PHONY: sn n cli