# Programs to use
CPLUS=g++
JAVAC = javac

# Directories, input and output files
SRCDIR=karmaresearch/vlog
NATIVEDIR=native
SRCFILES= $(SRCDIR)/VLog.java
NATIVEFILES=$(NATIVEDIR)/VLog.cpp
NATIVELIB=$(NATIVEDIR)/libvlog.jnilib
JAR=vlog.jar

# Various flags
JAVACFLAGS = -h $(NATIVEDIR)
CFLAGS=$(JNI) -c -fPIC -O3 -std=c++11

# Include files
CINCLUDES=-I../include -I../../kognac/include -I../../trident/include
# Libs
CLIBS=-L../build -L/Users/jacopo/prj/trident/build -L/Users/jacopo/prj/kognac/build -lkognac-log -lvlog -ltrident-core -ltrident-sparql

# Depend on the architecture. This works for mac.
JAVA_HOME=$(shell /usr/libexec/java_home)
JNI=-I $(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin

CLSFILES=$(SRCFILES:.java=.class)
ONATIVEFILES=$(NATIVEFILES:%.cpp=%.o)

all: $(CLSFILES) $(NATIVELIB) $(JAR)
	@echo "Done!"

$(JAR): $(CLSFILES)
	jar cf $@ $(CLSFILES)

%.class: %.java
	$(JAVAC) $(JAVACFLAGS) $<

$(NATIVELIB): $(ONATIVEFILES)
	$(CPLUS) $(CLIBS) -fPIC -shared -o $@ $(ONATIVEFILES)

%.o: %.cpp
	$(CPLUS) $(CFLAGS) $(CINCLUDES) -o $@ $<

clean:
	@rm -f $(SRCDIR)/*.class
	@rm -f $(NATIVEDIR)/*.h
	@rm -f $(ONATIVEFILES)
	@rm -f $(NATIVELIB)
	@rm -f $(JAR)
