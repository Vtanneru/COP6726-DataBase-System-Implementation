#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "GenericDB.h"
#include "GenericDB.cc"
#include "HeapDB.cc"
#include "SortedDB.cc"
#include "SortedDB.h"
#include "BigQ.h"
#include "BigQ.cc"
#include <stdlib.h>
#include "Defs.h"
using namespace std;

class DBFileTest : public ::testing::Test {
protected:
    DBFileTest() {
    }

    ~DBFileTest() override {
    }
    void SetUp() override {
        dbfile = new DBFile();
        orderMaker.numAtts = 1;
        orderMaker.whichAtts[0] = 4;
        orderMaker.whichTypes[0] = String;
    }

    void TearDown() override {
        delete dbfile;
    }
    DBFile* dbfile;
    OrderMaker orderMaker;
};

TEST_F(DBFileTest, FileCreation) {
    struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
    int aa = dbfile->Create("./gtest.bin", sorted, &startup);
    cout<<"=="<<aa<<endl;
    dbfile->Close();
}

TEST_F(DBFileTest, FileClose) {
    dbfile->Open("./gtest.bin");
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST_F(DBFileTest, FileCreateCloseTest) {
    struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
    dbfile->Create("./gtest.bin", sorted, &startup);
    EXPECT_EQ(dbfile->Close(), 1);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
