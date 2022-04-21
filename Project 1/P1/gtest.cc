#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include <stdlib.h>
using namespace std;

DBFile *dbfile;

TEST(DBFileTest, CreateTest) {
    ASSERT_EQ(0, dbfile->Create(NULL, heap, NULL));
}

TEST(DBFileTest, OpenTest) {
    EXPECT_EQ(dbfile->Open("gtest/lineitem.bin"), 1);
}

TEST(DBFileTest, CloseTest) {
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST(DBFileTest, OpenAndCloseTest) {
    dbfile->Open("gtest/lineitem.bin");
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST(DBFileTest, CreateAndCloseTest) {
    dbfile->Create("gtest/test.bin", heap, NULL);
    EXPECT_EQ(dbfile->Close(), 1);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}