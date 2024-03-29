#ifndef A2_2TEST_HEAPDB_H
#define A2_2TEST_HEAPDB_H
#include "GenericDB.h"

class HeapDB : public DBFileGeneric{
private:
    File diskFile;
    Page bufferPage;
    off_t pageIndex;
    int isWriting;
    int isFileOpen;

public:
    HeapDB ();

    int Create (char *fpath, fType f_type, void *startup);
    int Open (char *fpath);
    int Close ();

    void Load (Schema &myschema, char *loadpath);

    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};


#endif //A2_2TEST_DBFILEHEAP_H
