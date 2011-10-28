#
# Makefile for Minibase Projects
# CS 541, Fall 2011, Purdue University
#
# This Makefile was designed for Linux.
# For Windows (cygwin), change each : to \; in the variables below.
#

SRCPATH=src
BINPATH=bin
BUFJARS=lib/bufmgr.jar  
DSKJARS=lib/diskmgr.jar  
HEPJARS=lib/heap.jar  
INDJARS=lib/index.jar
SOLJARS=lib/heap.jar:lib/bufmgr.jar:lib/diskmgr.jar:lib/index.jar

JAVAC = javac -d $(BINPATH) -classpath $(SOLJARS)
JAVA  = java -classpath $(BINPATH):$(SOLJARS)

all: relop tests

relop:
	$(JAVAC) $(SRCPATH)/relop/*.java
	cp $(SRCPATH)/relop/*.class $(BINPATH)/relop/
tests:
	$(JAVAC) $(SRCPATH)/tests/*.java

hftest:
	$(JAVA) tests.ROTest


clean: clean_classes clean_backups clean_temps
	rm -rf *.minibase $(BINPATH)/*

clean_classes:
	\find ./bin -name \*.class -exec rm -f {} \;

clean_backups:
	\find ./bin -name \*~ -exec rm -f {} \;

clean_temps:
	\find ./bin -name \#* -exec rm -f {} \;

