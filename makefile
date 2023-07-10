CC = gcc
CPP = g++
CFLAGS = -Wall -g
 
distlink.o: distlink.cpp distlink.h
	$(CPP) $(CFLAGS) -fPIC -c distlink.cpp
	$(CPP) $(CFLAGS) -shared -o distlink.so distlink.o