#ifndef STATISTICS_H
#define STATISTICS_H
#include "ParseTree.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace std;
class Rel_Info;
typedef map<string, Rel_Info> RelMap;
class Attrib_Info;
typedef map<string, Attrib_Info> AttrMap;

class Attrib_Info {
public:
    int distinctTuples;
	string attrName;

    Attrib_Info (string name, int num);
	Attrib_Info ();
    Attrib_Info &operator= (const Attrib_Info &copyMe);
	Attrib_Info (const Attrib_Info &copyMe);
};

class Rel_Info {
public:
    bool isJoint;
	double numTuples;
    AttrMap attrMap;
	string relName;
	map<string, string> relJoint;
    Rel_Info (const Rel_Info &copyMe);
	Rel_Info ();
	Rel_Info &operator= (const Rel_Info &copyMe);
	bool isRelationPresent (string _relName);
    Rel_Info (string name, int tuples);
};

class Statistics_Data {
private:
    int GetRelForOp (Operand *operand, char *relName[], int numJoin, Rel_Info &relInfo);
    double ComOp (ComparisonOp *comOp, char *relName[], int numJoin);
	double AndOp (AndList *andList, char *relName[], int numJoin);
    double OrOp (OrList *orList, char *relName[], int numJoin);
public:
    ~Statistics_Data();
	RelMap relMap;
    Statistics_Data operator= (Statistics_Data &copyMe);
	Statistics_Data();
	Statistics_Data(Statistics_Data &copyMe);
    void Write(char *toWhere);
    void CopyRel(char *oldName, char *newName);
    void AddRel(char *relName, int numTuples);
    void Read(char *fromWhere);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
    bool isRelInMap (string relName, Rel_Info &relInfo);
	void AddAtt(char *relName, char *attrName, int numDistincts);
	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
};

#endif
