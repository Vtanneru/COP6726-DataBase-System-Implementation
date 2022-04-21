#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <iostream>


DBFile::DBFile () {

}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if(f_type==heap){
        Db_File.Open(0, const_cast<char *>(f_path));
        Page_Index = 0;
        is_Writing = 0;
        MoveFirst();
    }
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    ComparisonEngine comp;

    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
            this->Add(temp);
    }
    if (is_Writing == 1)
        Db_File.AddPage(&Page_Buffer, Page_Index);
}

int DBFile::Open (const char *f_path) {
    Db_File.Open(1, const_cast<char *>(f_path));
    Page_Index = 0;
    is_Writing = 0;
    MoveFirst();
    return 1;
}

void DBFile::MoveFirst () {
    if (is_Writing == 1) {
        Db_File.AddPage(&Page_Buffer, Page_Index);
        is_Writing = 0;
    }
    Page_Index = 0;
    Page_Buffer.EmptyItOut();
    if (Db_File.GetLength() > 0) {
        Db_File.GetPage(&Page_Buffer, Page_Index);
    }
    cout << "length of file is " << Db_File.GetLength() << "\n";
}

int DBFile::Close () {
    if (is_Writing == 1)
        Db_File.AddPage(&Page_Buffer, Page_Index);
    Db_File.Close();
    cout << "Closing file, length of file is " << Db_File.GetLength() << "Pages" << "\n";
    return 1;
}

void DBFile::Add (Record &rec) {
    if (is_Writing == 0) {
        Page_Buffer.EmptyItOut();
        if (Db_File.GetLength() > 0) {
            Db_File.GetPage(&Page_Buffer, Db_File.GetLength() - 2);
            Page_Index = Db_File.GetLength() - 2;
        }
        is_Writing = 1;
    }
    if (Page_Buffer.Append(&rec) == 0) {
        Db_File.AddPage(&Page_Buffer, Page_Index++);
        Page_Buffer.EmptyItOut();
        Page_Buffer.Append(&rec);
    }
}

int DBFile::GetNext (Record &fetchme) {
    if (Page_Buffer.GetFirst(&fetchme) == 0) {
        Page_Index++;
        if (Page_Index >= Db_File.GetLength() - 1) {
            return 0;
        }
        Page_Buffer.EmptyItOut();
        Db_File.GetPage(&Page_Buffer, Page_Index);
        Page_Buffer.GetFirst(&fetchme);
    }
    if (is_Writing == 1) {
        MoveFirst();
    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while (GetNext(fetchme) == 1) {
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}
