#include "BigQ.h"

void* workerMain(void* arg) {
    WorkerArg* workerArg = (WorkerArg*) arg;
    priority_queue<Execute*, vector<Execute*>, CompareRun> runQueue(workerArg->order);
    priority_queue<Record*, vector<Record*>, CompareRec> recordQueue (workerArg->order);
    vector<Record* > recBuff;
    Record curRecord;


    File file;
    char* fileName = "tmp.bin";
    file.Open(0, fileName);

    Page bufferPage;
    int pageIndex = 0;
    int pageCounter = 0;
 
    while (workerArg->in->Remove(&curRecord) == 1) {
        Record* tmpRecord = new Record;
        tmpRecord->Copy(&curRecord);
        if (bufferPage.Append(&curRecord) == 0) {
            pageCounter++;
            bufferPage.EmptyItOut();
            if (pageCounter == workerArg->runlen) {
                record_Queue(recordQueue, runQueue, file, bufferPage, pageIndex);
                recordQueue = priority_queue<Record*, vector<Record*>, CompareRec> (workerArg->order);
                pageCounter = 0;
            }

            bufferPage.Append(&curRecord);
        }

        recordQueue.push(tmpRecord);
    }
    if (!recordQueue.empty()) {
        record_Queue(recordQueue, runQueue, file, bufferPage, pageIndex);
        recordQueue = priority_queue<Record*, vector<Record*>, CompareRec> (workerArg->order);
    }
    while (!runQueue.empty()) {
        Execute* run = runQueue.top();
        runQueue.pop();
        workerArg->out->Insert(run->bestRecord);
        if (run->UpdatebestRecord() == 1) {
            runQueue.push(run);
        }
    }
    file.Close();
    workerArg->out->ShutDown();
    return NULL;
}
void* record_Queue(priority_queue<Record*, vector<Record*>, CompareRec>& recordQueue, 
    priority_queue<Execute*, vector<Execute*>, CompareRun>& runQueue, File& file, Page& bufferPage, int& pageIndex) {

    bufferPage.EmptyItOut();
    int startIndex = pageIndex;
    while (!recordQueue.empty()) {
        Record* tmpRecord = new Record;
        tmpRecord->Copy(recordQueue.top());
        recordQueue.pop();
        if (bufferPage.Append(tmpRecord) == 0) {
            file.AddPage(&bufferPage, pageIndex++);
            bufferPage.EmptyItOut();
            bufferPage.Append(tmpRecord);
        }
    }
    file.AddPage(&bufferPage, pageIndex++);
    bufferPage.EmptyItOut();
    Execute* run = new Execute(&file, startIndex, pageIndex - startIndex);
    runQueue.push(run);
    return NULL;
}



Big_Q :: Big_Q (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    pthread_t worker;
    WorkerArg* workerArg = new WorkerArg;
    workerArg->in = &in;
    workerArg->out = &out;
    workerArg->order = &sortorder;
    workerArg->runlen = runlen;
    pthread_create(&worker, NULL, workerMain, (void*) workerArg);
    pthread_join(worker, NULL);
	out.ShutDown ();
}

Big_Q::~Big_Q () {

}

Execute::Execute(File* file, int start, int length) {
    fileBase = file;
    startPage = start;
    runLength = length;
    curPage = start;
    fileBase->GetPage(&bufferPage, startPage);
    bestRecord = new Record;
    UpdatebestRecord();
}

int Execute::UpdatebestRecord() {
    if (bufferPage.GetFirst(bestRecord) == 0) {
        curPage++;
        if (curPage == startPage + runLength) {
            return 0;
        }
        bufferPage.EmptyItOut();
        fileBase->GetPage(&bufferPage, curPage);
        bufferPage.GetFirst(bestRecord);
    }
    return 1;
}

CompareRec::CompareRec(OrderMaker* orderMaker) {
    order = orderMaker;
}

bool CompareRec::operator () (Record* left, Record* right) {
    ComparisonEngine comparisonEngine;
    if (comparisonEngine.Compare(left, right, order) >= 0)
        return true;
    return false;
}
 CompareRun:: CompareRun(OrderMaker* orderMaker) {
    order = orderMaker;
}

bool CompareRun::operator () (Execute* left, Execute* right) {
    ComparisonEngine comparisonEngine;
    if (comparisonEngine.Compare(left->bestRecord, right->bestRecord, order) >= 0)
        return true;
    return false;
}


