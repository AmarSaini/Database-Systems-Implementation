#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include <fstream>

using namespace std;


class DBFile {
private:
	File file;
	string fileName;
	Page temp;
	int count;
	ifstream inFile;
	Page p;

public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

	int Create (char* fpath, FileType file_type);
	int Open (char* fpath);
	int Close ();

	void Load (Schema& _schema, char* textFile);

	void MoveFirst ();
	void AppendRecord (Record& _addMe);
	
	void AppendLast();
	
	int GetNext (Record& _fetchMe);

	
};

class IndexFile : DBFile
{
private:
	File myIndexFile;
	File file;			// just there?
	string fileName;	// for myIndexFile
	Page temp;
	int count;
	ifstream inFile;
	Page p;

public:
// Index Function
	IndexFile();
	IndexFile(File& f);
	int GetNextIndex(Record& _fetchMe, CNF& predicate);
	void AppendLastIndex(int& nodeType, int& parentNum);
	void AppendRecordIndex(Record& _addMe, int& nodeType, int& parentNum);
	int OpenIndex(char* f_path);	// index file version
	int CloseIndex();
	int CreateIndex(char* f_path, FileType f_type);
};

#endif //DBFILE_H
