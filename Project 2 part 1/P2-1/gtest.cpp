#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "BigQ.h"
#include "BigQ.cc"
#include <stdlib.h>
using namespace std;

class BigQTest : public ::testing::Test {
protected:
    BigQTest() {
    }

    ~BigQTest() override {
    }
    void SetUp() override {
    }

    void TearDown() override {
    }
    char* regionFileName = "test/region.bin";
    char* lineitemFileName = "test/lineitem.bin";
    char* testFileName = "test/test.bin";
};

TEST_F(BigQTest, UpdateTopRecordForRunTest) {
    File file;
    file.Open(1, regionFileName);
    class Execute* run = new class Execute(&file, 0, 1);
    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record tempRecord;
    bufferPage.GetFirst(&tempRecord);
    while (bufferPage.GetFirst(&tempRecord) == 1) {
        EXPECT_EQ(run->UpdatebestRecord(), 1);
    }
    EXPECT_EQ(run->UpdatebestRecord(), 0);
    file.Close();
}

TEST_F(BigQTest, RecordComparerTest) {
    File file;
    file.Open(1, regionFileName);
    Schema* scheme = new Schema("catalog", "region");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<Record*, vector<Record*>, CompareRec> recordQueue (order);
    ComparisonEngine comparisonEngine;
    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record* readindRecord = new Record;
    while (bufferPage.GetFirst(readindRecord)) {
        recordQueue.push(readindRecord);
        readindRecord = new Record;
    }

    bufferPage.EmptyItOut();
    file.GetPage(&bufferPage, 0);
    Record rec[2];
    Record *last = NULL, *prev = NULL;
    int i = 0;
    while (bufferPage.GetFirst(&rec[i%2]) == 1) {
        prev = last;
        last = &rec[i%2];
        if (prev && last) {
            EXPECT_EQ(comparisonEngine.Compare(prev, last, order), -1);
        }
        i++;
    }
    file.Close();
}

TEST_F(BigQTest, RunComparerTest) {
    File file;
    file.Open(1, lineitemFileName);
    Schema* scheme = new Schema("catalog", "lineitem");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<class Execute*, vector<class Execute*>, CompareRun> runQueue (order);
    ComparisonEngine comparisonEngine;

    class Execute* run1 = new class Execute(&file, 0, 1);
    class Execute* run2 = new class Execute(&file, 1, 1);
    runQueue.push(run1);
    runQueue.push(run2);

    Record one, two;
    one.Copy(runQueue.top()->bestRecord);
    runQueue.pop();
    two.Copy(runQueue.top()->bestRecord);
    runQueue.pop();
    EXPECT_EQ(comparisonEngine.Compare(&one, &two, order), -1);

    file.Close();
}

TEST_F(BigQTest, recordToRunTest) {
    File file;
    file.Open(1, regionFileName);
    Schema* scheme = new Schema("catalog", "region");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<Record*, vector<Record*>, CompareRec> recordQueue (order);
    ComparisonEngine comparisonEngine;
    Page bufferPage;
    file.GetPage(&bufferPage, 0);
    Record* readindRecord = new Record;
    while (bufferPage.GetFirst(readindRecord)) {
        recordQueue.push(readindRecord);
        readindRecord = new Record;
    }
    File testFile;
    testFile.Open(0, testFileName);
    Page testPage;
    int pageIndex = 0;
    priority_queue<class Execute*, vector<class Execute*>, CompareRun> runQueue (order);
    record_Queue(recordQueue, runQueue, file, bufferPage, pageIndex);
    EXPECT_EQ(recordQueue.size(), 0);
    EXPECT_EQ(runQueue.size(), 1);
    file.Close();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
