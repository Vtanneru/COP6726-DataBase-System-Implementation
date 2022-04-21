
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

a4-1.out: Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o a4-1.out Statistics.o y.tab.o lex.yy.o test.o -ll

gtest: Statistics.o Function.o y.tab.o lex.yy.o gtest.o 
	$(CC) -o gtest.out Statistics.o y.tab.o lex.yy.o gtest.o -std=c++11 -pthread -lgtest -lgtest_main

gtest.o: gtest.cpp
	$(CC) -g -c gtest.cpp -std=c++11 -pthread -lgtest -lgtest_main

QueryTreeNode.o: QueryTreeNode.cc
	$(CC) -g -c QueryTreeNode.cc

RelationStatistics.o: RelationStatistics.cc
	$(CC) -g -c RelationStatistics.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

main.o : main.cc
	$(CC) -g -c main.cc

main1.o : main1.cc
	$(CC) -g -c main1.cc

QueryTreeOperations.o : QueryTreeOperations.cc
	$(CC) -g -c QueryTreeOperations.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  _attribute_ ((_unused))$$/# ifndef __cplusplus\n  __attribute_ ((_unused_));\n# endif/" 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
# sed $(tag) yyfunc.tab.c -e "s/  _attribute_ ((_unused))$$/# ifndef __cplusplus\n  __attribute_ ((_unused_));\n# endif/" 
	g++ -c yyfunc.tab.c
	
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h