#
# Makefile for Minibase Projects
# CS 541, Fall 2011, Purdue University
#
# This Makefile was designed for Linux.
# For Windows (cygwin), change each : to \; in the variables below.
#

SRCPATH=src
BINPATH=bin
SOLJARS=lib/heap.jar:lib/bufmgr.jar:lib/diskmgr.jar:lib/index.jar

JAVAC = javac -d $(BINPATH) -classpath $(BINPATH):$(SOLJARS)
JAVA  = java -classpath $(BINPATH):$(SOLJARS)

all: global relop tests

global:
	$(JAVAC) $(SRCPATH)/global/*.java
relop:
	$(JAVAC) $(SRCPATH)/relop/*.java
tests:
	$(JAVAC) $(SRCPATH)/tests/*.java

rotest:
	$(JAVA) tests.ROTest


clean: clean_classes clean_backups clean_temps
	rm -rf *.minibase $(BINPATH)/*

clean_classes:
	\find ./bin -name \*.class -exec rm -f {} \;

clean_backups:
	\find ./bin -name \*~ -exec rm -f {} \;

clean_temps:
	\find ./bin -name \#* -exec rm -f {} \;

