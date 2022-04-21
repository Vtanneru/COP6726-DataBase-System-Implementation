#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class Big_Q {

public:

	Big_Q (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~Big_Q ();

};

class Execute {

public:
	Execute(File* file, int startPage, int runLength);
	int UpdatebestRecord();
	Record *bestRecord; 

private: 
	File* fileBase;
	Page bufferPage;
	int startPage;
	int runLength;
	int curPage;
};
class CompareRec {

public:
	bool operator () (Record* left, Record* right);
	CompareRec(OrderMaker *order);

private:
	OrderMaker *order;

};
class CompareRun {

public:
	bool operator () (Execute* left, Execute* right);
	CompareRun(OrderMaker *order);

private:
	OrderMaker *order;

};
typedef struct {
	
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
	
} WorkerArg;
void* Work_func(void* arg);
void* record_Queue(priority_queue<Record*, vector<Record*>, CompareRec>& recordQueue, 
    priority_queue<Execute*, vector<Execute*>, CompareRun>& runQueue, File& file, Page& bufferPage, int& pageIndex);

#endif
