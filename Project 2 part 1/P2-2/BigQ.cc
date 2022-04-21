#include "BigQ.h"
#include "GenericDB.h"

Big_Q :: Big_Q (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    cout<< "begin BigQ" << endl;
    pthread_t worker;
    WorkerArg* Argmnt = new WorkerArg;
    Argmnt->in = &in;
    Argmnt->out = &out;
    Argmnt->order = &sortorder;
    Argmnt->runlen = runlen;
    pthread_create(&worker, NULL, Work_func, (void*) Argmnt);
    cout<< "end BigQ" << endl;
    // pthread_join(worker, NULL);
	// out.ShutDown ();
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

void* Work_func(void* arg) {
    cout<<"Work_func Starts" << endl;
    WorkerArg* Argmnt = (WorkerArg*) arg;
    priority_queue<Execute*, vector<Execute*>, CompareRun> runQueue(Argmnt->order);
    priority_queue<Record*, vector<Record*>, CompareRec> recordQueue (Argmnt->order);
    vector<Record* > recBuff;
    Record curRecord;


    File file1;
    char* fileName = "tmp.bin";
    file1.Open(0, fileName);

    Page bufferPage;
    int pageIndex = 0;
    int pageCounter = 0;
 
    while (Argmnt->in->Remove(&curRecord) == 1) {
        Record* tmpRecord = new Record;
        tmpRecord->Copy(&curRecord);
        if (bufferPage.Append(&curRecord) == 0) {
            pageCounter++;
            bufferPage.EmptyItOut();
            if (pageCounter == Argmnt->runlen) {
                record_Queue(recordQueue, runQueue, file1, bufferPage, pageIndex);
                recordQueue = priority_queue<Record*, vector<Record*>, CompareRec> (Argmnt->order);
                pageCounter = 0;
            }

            bufferPage.Append(&curRecord);
        }

        recordQueue.push(tmpRecord);
    }
    if (!recordQueue.empty()) {
        record_Queue(recordQueue, runQueue, file1, bufferPage, pageIndex);
        recordQueue = priority_queue<Record*, vector<Record*>, CompareRec> (Argmnt->order);
    }
     DBFile dbFileHeap;
    cout<< "1" << endl;
    dbFileHeap.Create("tempDifFile.bin", heap, nullptr);
    Record rec;
    while (!runQueue.empty()) {
        Execute* run = runQueue.top();
        runQueue.pop();
        dbFileHeap.Add(*(run->bestRecord));
        // Argmnt->out->Insert(run->bestRecord);
        if (run->UpdatebestRecord() == 1) {
            runQueue.push(run);
        }
    }
    dbFileHeap.Close();
    file1.Close();
    Argmnt->out->ShutDown();
    cout<<"end Work_func" << endl;
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


