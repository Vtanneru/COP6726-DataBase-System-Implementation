
CC = g++ -std=c++11 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDB.o  HeapDB.o SortedDB.o Pipe.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDB.o  HeapDB.o SortedDB.o Pipe.o y.tab.o lex.yy.o test.o -lpthread #-lfl

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o main.o
	$(CC) -o main.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o main.o -lpthread #-lfl
	
a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDB.o Pipe.o y.tab.o lex.yy.o a2-test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o GenericDB.o Pipe.o y.tab.o lex.yy.o a2-test.o -lpthread # -lfl
	
a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDB.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDB.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

test.o: test.cc
	$(CC) -g -c test.cc

main.o: main.cc
	$(CC) -g -c main.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

GenericDB.o: GenericDB.cc
	$(CC) -g -c GenericDB.cc

HeapDB.o: HeapDB.cc
	$(CC) -g -c HeapDB.cc

SortedDB.o: SortedDB.cc
	$(CC) -g -c SortedDB.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Pipe.o y.tab.o lex.yy.o gtest.o
	$(CC) -o gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o Pipe.o y.tab.o lex.yy.o gtest.o -std=c++14 -pthread -lgtest -lgtest_main

gtest.o: gtest.cpp
	$(CC) -g -c gtest.cpp -std=c++14 -pthread -lgtest -lgtest_main

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	#sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f *.bin
	rm -f *.meta
