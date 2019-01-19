#include <iostream>
#include <fstream>
#include <sstream>			// used for var to str conversion
#include <string.h>			// used for memcpy and memmove
#include <stdio.h>
#include <stdlib.h>
#include "RelOp.h"
#include "EfficientMap.h"
#include "EfficientMap.cc"
#include "Keyify.h"
#include "Config.h"
#include "RecordMinHeap.h"
#include "RecordMinHeap.cc"
using namespace std;

ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}
int counterEQ = 0;
void QueryExecutionTree::ExecuteQuery() {
	Record rec;
	// Delete Me
	//cout << "Execution Tree Counter: " << counterEQ++ << endl;
	while (root->GetNext(rec)) {	//call getNext of root until there are no more tuples
		//cout << "Execution Tree Counter: " << counterEQ++ << endl;
	}
}

Scan::Scan(Schema& _schema, DBFile& _file, string table) {
	schema = _schema;
	s = _schema;
	file = _file;
	this->table = table;
	counter = 0;
}

bool Scan::GetNext(Record& _record) {
	//cout << "Scan Counter: " << counter++ << endl;
	//if (counter > 100)
	//	exit(0);
	if(file.GetNext(_record)) {
		sum += _record.GetSize();
		_sum += _record.GetSize();
		//std::cout << "rel op running sum: " << RelationalOp::getSum() << std::endl;
		//std::cout << "scan running sum " << sum << std::endl;
		return true;
	}
	return false;
}

Scan::Scan()
{
	schema = Schema();
	file =  DBFile();
	counter = 0;
}

Scan::~Scan() {

}

Schema& Scan::getSchema()
{
	return schema;
}

DBFile& Scan::getFile()
{
	return file;
}

string Scan::getTable() {
	return table;
}

void Scan::Swap(Scan& withMe)
{
	SWAP(schema, withMe.getSchema());
	SWAP(file, withMe.getFile());
}

void Scan::CopyFrom(Scan& withMe)
{
	this->schema = withMe.schema;
	this->file = withMe.file;
}

ostream& Scan::print(ostream& _os) {
	return _os << "SCAN\nSchema:" << schema << "\nFile\nSum: " << sum << std::endl;
}

Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer, string table) {
	schema = _schema;
	s = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
	this->table = table;

	counter = 0;
}

Select::Select()
{
	counter = 0;
}

Select::~Select() {

}

Schema& Select::getSchema()
{
	return schema;
}

CNF& Select::getCNF()
{
	return predicate;
}

Record& Select::getRecord()
{
	return constants;
}

RelationalOp* Select::getRelational()
{
	return producer;
}

string Select::getTable() {
	return table;
}

void Select::Swap(Select& withMe)
{
	SWAP(schema, withMe.schema);
	SWAP(predicate, withMe.predicate);
	SWAP(constants, withMe.constants);
	SWAP(producer, withMe.producer);
}

ostream& Select::print(ostream& _os) {
	return _os << "SELECT\nSchema: " << schema << "\nPredicate: " << predicate << "\nProducer: " << *producer << endl;
}

bool Select::GetNext(Record& _record) {
	//Record record;
	//cout << "Select Counter: " << counter++ << endl;
	while (producer->GetNext(_record)) {
		if (predicate.Run(_record, constants)) {	// constants = literals?
			//_record = record;
			sum += _record.GetSize();
			_sum += _record.GetSize();
			//std::cout << "running sum: " << _sum << std::endl;
			return true;
		}
	}
	return false;
}

Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
s = _schemaOut;
schemaIn = _schemaIn;
schemaOut = _schemaOut;
numAttsInput = _numAttsInput;
numAttsOutput = _numAttsOutput;
keepMe = _keepMe;
producer = _producer;
counter = 0;
}

Project::~Project() {

}

ostream& Project::print(ostream& _os) {
	_os << "PROJECT: \nSchema In: " << schemaIn << "\nSchema Out: " << schemaOut << "\n# Attributes Input: " << numAttsInput << "\n# Attributes Output: " << numAttsOutput << "\nKeep: ";
	for (int i = 0; i < numAttsOutput; i++) {
		_os << keepMe[i] << " ";
	}
	return _os << "\nProducer: " << *producer << endl;
}

bool Project::GetNext(Record& _record) {
	// Assume Project is working correctly
	// that is every private member variable is holding what it describes in header file
	if (producer->GetNext(_record)) {
		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;
	}
	return false;
}

Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right, double _leftMem, double _rightMem, double _memCapacity, int _joinCount) {
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	s = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;

	leftMem = _leftMem;
	rightMem = _rightMem;
	joinCount = _joinCount;

	appendIndex = 0;
	buildCheck = true;
	fitsInMemory = true;
	firstTimeBuild = true;
	memCapacity = _memCapacity;
	finishMerge = true;

	fileNum = 0;

	std::cout << "left sizeeeee: " << _leftMem << "right sizeeeee: " << _rightMem << std::endl;
	//joinComp.Swap(_joinComp);
}

Join::~Join() {
	if (leftHeap) {
		delete leftHeap;
	}

	if (rightHeap) {
		delete rightHeap;
	}
}

bool Join::GetNext(Record& _record) {
	//std::cout << "lefttuples: "  << leftTuples << std::endl;
	//std::cout << "righttuples: " << rightTuples << std::endl;
	//std::cout << "memCapacity: " << memCapacity << std::endl;
	if (buildCheck) {
		buildCheck = false;
		if (leftMem < rightMem) {
			cout << "Left is Small\n";
			cout << "Left Schema" << schemaLeft << endl;
			cout << "Right Schema" << schemaRight << endl;
			mergeJoin(memCapacity, 0);
			if (!fitsInMemory) {
				HangMerge();
			}
		}
		else {
			cout << "Right is Small\n";
			cout << "Right Schema" << schemaRight << endl;
			cout << "Left Schema" << schemaLeft << endl;
			mergeJoin(memCapacity, 1);
			if(!fitsInMemory) {
				HangMerge();	
			}
		}
	}
	/*if (appendIndex < appendRecords.size())
	{
		_record = *appendRecords[appendIndex];
		appendIndex++;
		//cout << "Returned: " << appendIndex << endl;
		return true;
	}
	else
	{
		return false;
	}*/
	if (fitsInMemory) {
		if (appendIndex < appendRecords.size())
		{
			_record = *appendRecords[appendIndex];
			appendIndex++;
			//cout << "Returned: " << appendIndex << endl;
			return true;
		}
		else
		{
			return false;
		}
	}
	else {
		if (appendedRecords.GetNext(_record) == 1) {
			return true;
		}
		else {
			appendedRecords.Close();
			ostringstream oss;
			string result;
			string name = "appended";
			string bin = ".bin";
			oss << startLoc << name << joinCount << bin;
			result = oss.str();
			remove(result.c_str());
			return false;
		}
	}
}

bool Join::writeDisk(RelationalOp* producer, OrderMaker side, int sideName) {

	Record recTemp;
	//left side first cuz normal people go left to right, not right to left. Freaking amar
	KeyInt keyTemp = KeyInt(1);

	double memUsed = 0.0;
	EfficientMap <Record, KeyInt> tempMap;
	bool lastCheck = false;


	std::cout << "Entering while loop: " << std::endl;
	startLoc = "../Disk/";
	// Build

	while (producer->GetNext(recTemp))
	{

		//cout << "help" << endl;
		//std::cout << "Entered while loop: " << std::endl;
		lastCheck = true;

		//std::cout << "attempting to insert record: " << std::endl;
		//recTemp.print(cout, schemaLeft);
		//std::cout << "." << std::endl;

		recTemp.SetOrderMaker(&side);

		memUsed += recTemp.GetSize();
		//std::cout << "memUsed: " << memUsed << std::endl;
		keyTemp = KeyInt(1);
		//std::cout << "created keyint" << std::endl;
		tempMap.Insert(recTemp, keyTemp);
		//std::cout << "Successful insertion" << std::endl;

		//std::cout << "derp" << std::endl;
		//std::cout << "derp" << std::endl;
		

		if (memUsed > memCapacity) // flush
		{

			fitsInMemory = false;

			memUsed = 0;

			std::cout << "Flushing While" << std::endl;

			DBFile myOutput;

			FileType file_type;
			file_type = Heap;

			ostringstream oss;
			
			string loc;
			if (sideName < 1)
				loc = startLoc + "left";
			else
				loc = startLoc + "right";
			string bin = ".bin";

			oss << loc << joinCount << fileNum << bin;


			fileNum++;

			string myOutputFile = oss.str();

			char* myOutputFileC = new char[myOutputFile.length() + 1];
			strcpy(myOutputFileC, myOutputFile.c_str());

			myOutput.Create(myOutputFileC, file_type);

			tempMap.MoveToStart();

			while (!tempMap.AtEnd()) {

				myOutput.AppendRecord(tempMap.CurrentKey());

				tempMap.Advance();

			}

			myOutput.AppendLast();

			myOutput.Close();



			/*DBFile testOutput;

			testOutput.Open(myOutputFileC);

			Record testtt;

			while (testOutput.GetNext(testtt) == 1) {

				testtt.print(std::cout, schemaLeft);
				std::cout << std::endl;

			}

			testOutput.Close();*/

			return true;

		}

	}

	cout << "out of loop" << endl;
	cout << fitsInMemory << endl;

	// Last page, write out

	if (lastCheck && !fitsInMemory) {

		memUsed = 0;

		std::cout << "Flushing Last Check" << std::endl;

		DBFile myOutput;

		FileType file_type;
		file_type = Heap;

		ostringstream oss;

		string loc;
		if (sideName < 1)
			loc = startLoc + "left";
		else
			loc = startLoc + "right";
		string bin = ".bin";

		oss << loc << joinCount << fileNum << bin;


		fileNum++;

		string myOutputFile = oss.str();

		char* myOutputFileC = new char[myOutputFile.length() + 1];
		strcpy(myOutputFileC, myOutputFile.c_str());

		myOutput.Create(myOutputFileC, file_type);

		tempMap.MoveToStart();

		while (!tempMap.AtEnd()) {

			myOutput.AppendRecord(tempMap.CurrentKey());
			
			tempMap.Advance();

		}

		myOutput.AppendLast();

		myOutput.Close();

		
		/*DBFile testOutput;

		testOutput.Open(myOutputFileC);

		Record testtt;

		while (testOutput.GetNext(testtt) == 1) {


			if (sideName == 1) {

				testtt.print(std::cout, schemaRight);
				std::cout << std::endl;

			}

		}

		testOutput.Close();
		*/
		
		return false;

	}

	if (fitsInMemory) {

		
		tempMap.MoveToStart();


		if (sideName == 0) {
			cout << "Probing right" << endl;
			inMem(tempMap, right, 0);
		}

		else if (sideName == 1) {
			cout << "Probing left" << endl;
			inMem(tempMap, left, 1);
		}

		return false;

	}

}

void Join::mergeJoin(double memCapacity, int smallerSide)
{


		if (predicate.GetSortOrders(leftComp, rightComp) == 0) {

			cout << "Could not get Sort Orders" << endl;

		}


		// Do Left First

		if (smallerSide == 0) {

			while (writeDisk(left, leftComp, 0)) {	//0 for left string

				cout << fileNum << " Left Files" << endl;

			}

			cout << fileNum << " Left Files" << endl;

			leftFileNum = fileNum;
			fileNum = 0;

			if (!fitsInMemory) {


				while (writeDisk(right, rightComp, 1)) {

					cout << fileNum << " Right Files" << endl;

				}

				cout << fileNum << " Right Files" << endl;

				rightFileNum = fileNum;
				fileNum = 0;

			}

		}

		// Do Right First

		else if (smallerSide == 1) {

			while (writeDisk(right, rightComp, 1)) {	//0 for left string

				cout << fileNum << " Right Files" << endl;

			}

			cout << fileNum << " Right Files" << endl;

			rightFileNum = fileNum;
			fileNum = 0;

			if (!fitsInMemory) {


				while (writeDisk(left, leftComp, 0)) {

					cout << fileNum << " Left Files" << endl;

				}

				cout << fileNum << " Left Files" << endl;

				leftFileNum = fileNum;
				fileNum = 0;

			}

		}


}

void Join::inMem(EfficientMap<Record, KeyInt>& memRecords, RelationalOp* producer, int sideNum)
{

		Record temp;

		// Probe

		int counter = 0;
		int matchedC = 0;

		cout << "Starting to probe" << endl;

		while (producer->GetNext(temp))
		{
			//sum += temp.GetSize();	//summing left size
			//_sum += temp.GetSize();

			//cout << "Fetching tuple" << endl;

			memRecords.MoveToStart();

			//cout << "Fetching tuple #" << counter++ << endl;
			//temp.print(cout, schemaRight);
			//cout << endl;
			

			while(!memRecords.AtEnd())
			{

				Record* recordPt = new Record();
				*recordPt = memRecords.CurrentKey();

				//recordPt->print(cout, schemaLeft);
				//cout << endl;

				/*if (sideNum == 0) {

					cout << "Left record: ";
					recordPt->print(cout, schemaLeft);
					cout << endl;

					cout << "Right record: ";
					temp.print(cout, schemaRight);
					cout << endl;

				}

				else if (sideNum == 1) {

					cout << "Left record: ";
					temp.print(cout, schemaLeft);
					cout << endl;

					cout << "Right record: ";
					recordPt->print(cout, schemaRight);
					cout << endl;

				}*/
				if (sideNum == 0) {

					if (predicate.Run(*recordPt, temp))
					{
						//cout << "Found match at " << i << endl;
						Record merge;

						//cout << "Matched " << matchedC++ << endl;

						merge.AppendRecords(*recordPt, temp, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());

						//cout << "Appended" << endl;
						Record* merge2 = new Record();
						*merge2 = merge;
						appendRecords.push_back(merge2);
						//cout << "Pushed" << endl;
					}

				}

				else if (sideNum == 1) {

					if (predicate.Run(temp, *recordPt))
					{
						//cout << "Found match at " << i << endl;
						Record merge;

						//cout << "Matched " << matchedC++ << endl;

						merge.AppendRecords(temp, *recordPt, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());

						//cout << "Appended" << endl;
						Record* merge2 = new Record();
						*merge2 = merge;
						appendRecords.push_back(merge2);
						//cout << "Pushed" << endl;
					}

				}
				

				delete recordPt;

				memRecords.Advance();

			}
		}

		cout << "Done Probing" << endl;

}

void Join::InsertionSort(vector<HeapNode*>& toSort, OrderMaker& toOrder) {
	int size = toSort.size();
	for (int i = 0; i < size; ++i) {
		int j = i;

		while (j > 0 && (toOrder.Run(toSort[j - 1]->data, toSort[j]->data) == 1)) {
			HeapNode * temp = toSort[j];
			toSort[j] = toSort[j - 1];
			toSort[j - 1] = temp;
			--j;
		}
	}
}

/*DBFile testOutput;

testOutput.Open(myOutputFileC);

Record testtt;

while (testOutput.GetNext(testtt) == 1) {

testtt.print(std::cout, schemaLeft);
std::cout << std::endl;

}

testOutput.Close();*/

void Join::HangMerge() {
	// Number of runs for each side
	//String for File Names
	string left = "left";
	string right = "right";
	string bin = ".bin";
	if (leftFileNum == 0 || rightFileNum == 0) {
		for (int i = 0; i < leftFileNum; ++i) {
			ostringstream oss;
			string result;
			oss << startLoc << left << joinCount << i << bin;
			result = oss.str();
			remove(result.c_str());
		}
		for (int i = 0; i < rightFileNum; ++i) {
			ostringstream oss;
			string result;
			oss << startLoc << right << joinCount << i << bin;
			result = oss.str();
			remove(result.c_str());
		}
		return;
	}
	
	// DEBUGING ONLY
	//startLoc = "../Disk/";

	
	// Temporary temp file for DBFile
	DBFile* tempDB;										
	char * fileNameC;
	HeapNode * temp;
	int minLeftIndex, minRightIndex;									// Index of where the minimum came from for Left/RightBucket to choose which GetNext
	int heapExtractLeft, heapExtractRight;								// Holds 0 if previous heap extract fails, 1 if extract works
	
	if (firstTimeBuild) {
		// Setting up the Heap Tree to be used
		leftHeap = new MinHeap(leftComp);
		rightHeap = new MinHeap(rightComp);
		// Setting up the DBFile
		for (int i = 0; i < leftFileNum; ++i) {
			ostringstream oss;
			string result;
			oss << startLoc << left << joinCount << i << bin;
			result = oss.str();
			cout << result << endl;
			fileNameC = new char[result.length() + 1];
			strcpy(fileNameC, result.c_str());
			tempDB = new DBFile();
			tempDB->Open(fileNameC);
			leftFiles.push_back(tempDB);
		}

		for (int i = 0; i < rightFileNum; ++i) {
			ostringstream oss;
			string result;
			oss << startLoc << right << joinCount << i << bin;
			result = oss.str();
			cout << result << endl;
			fileNameC = new char[result.length() + 1];
			strcpy(fileNameC, result.c_str());
			tempDB = new DBFile();
			tempDB->Open(fileNameC);
			rightFiles.push_back(tempDB);
		}

		// Extracting the first element from each runs into Left/Right Bucket for the first time
		cout << "About to Extract First Left Element" << endl;
		for (int i = 0; i < leftFileNum; ++i) {
			cout << "Left i " << i << " ";
			Record temp;
			if (!leftFiles[i]->GetNext(temp)) {
				cerr << "Error extracting first element from leftFiles number " << i << endl;
				exit(0);
			}
			temp.print(cout, schemaLeft);
			cout << endl;
			leftHeap->insert(temp, i);
		}
		cout << "About to Extract First Right Element" << endl;
		for (int i = 0; i < rightFileNum; ++i) {
			cout << "Right i " << i << " ";
			Record temp;
			if (!rightFiles[i]->GetNext(temp)) {
				cerr << "Error extracting first element from rightFiles number " << i << endl;
				exit(0);
			}
			temp.print(cout, schemaRight);
			cout << endl;
			rightHeap->insert(temp, i);
		}
		
		// Set flag so any calls on this functions again will not go into this loop
		firstTimeBuild = false;
	}

	HeapNode *leftMin, *rightMin;

	// Creating output file

	DBFile myOutput;

	FileType file_type;
	file_type = Heap;

	ostringstream oss;

	string name = "appended";

	oss << startLoc << name << joinCount << bin;

	string myOutputFile = oss.str();

	char* myOutputFileC = new char[myOutputFile.length() + 1];
	strcpy(myOutputFileC, myOutputFile.c_str());

	myOutput.Create(myOutputFileC, file_type);


	// Debuging only
	unsigned int loopCounter = 0;
	unsigned int mergeCounter = 0;
	unsigned int LRcounter = 0;
	unsigned int RLcounter = 0;
	/*
	// Testing logic
	// prints correctly
	cout << "Extracting Min from leftHeap: ";
	leftMin = leftHeap->extractMin(heapExtractLeft);
	cout << "heapExtractLeft is " << heapExtractLeft << endl;
	leftMin->data.print(cout, schemaLeft);
	cout << "\nIndex: " << leftMin->index << endl;
	cout << "Extracting Min from rightHeap: ";
	rightMin = rightHeap->extractMin(heapExtractRight);
	cout << "heapExtractRight is " << heapExtractRight << endl;
	rightMin->data.print(cout, schemaRight);
	cout << "\nIndex: " << rightMin->index << endl;
	
	cout << "Check if Join arugment is the same: ";;
	Record* merge;
	int compResult = leftComp.Run(leftMin->data, rightMin->data, rightComp);
	cout << compResult << endl;
	if (compResult == 0) {
		cout << "Left Equal Right" << endl;
		merge = new Record();
		merge->AppendRecords(leftMin->data, rightMin->data, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
		cout << "Merged Record is: ";
		merge->print(cout, schemaOut);
		cout << endl;
		appendRecords.push_back(merge);
	}
	*/
	leftMin = leftHeap->extractMin(heapExtractLeft);
	minLeftIndex = leftMin->index;
	rightMin = rightHeap->extractMin(heapExtractRight);
	minRightIndex = rightMin->index;
	// Make sure previous heap extraction was valid for both table
	while (heapExtractLeft && heapExtractRight) {
		//cout << "Start of while Loop" << endl;
		//if (mergeCounter > 2) {
		//	break;
		//}
		//cout << "Loop " << loopCounter++ << endl;
		Record* merge;
		//cout << "Start First comparison" << endl;
		int compResult = leftComp.Run(leftMin->data, rightMin->data, rightComp);
		//cout << "End First Comparison" << endl;
		// Case where Join Condition is the same
		if (compResult == 0) {
			//cout << "Left Equal Right" << " Merge counter " << mergeCounter++ << endl;
			//leftMin->data.print(cout, schemaLeft);
			//cout << endl;
			//rightMin->data.print(cout, schemaRight);
			//cout << endl;
			// Vector to store case where next min tuple also has the same Joining value as current tuple 
			// and thus requiring a join with all existing tuples
			// example: leftMin.Joining = {1,1,1,2} and rightMin.joining = {1,1,1,3}
			// Each tuple of leftMin with joining value = 1 has to join with ALL of rightMin tuples
			// with joining value = 1
			vector<HeapNode*> tempLeft, tempRight;
			// Pushing first records into storage
			tempLeft.push_back(leftMin);
			tempRight.push_back(rightMin);
			//merge->AppendRecords(leftMin->data, rightMin->data, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
			// Loop through left heap extracting until joining value is different from tempLeft[0].Joining
			// Repeat for rightHeap
			//minLeftIndex = leftMin->index;
			//minRightIndex = rightMin->index;
			int compResultTemp = 0;
			int tempCounter = 0;
			//cout << "Start Extracting Left " << endl;
			while (heapExtractLeft && compResultTemp == 0) {
				//cout << "Extracting Left " << tempCounter++ << endl;
				// Pushing next tuple into left heap using minLeftIndex
				// Only push IF there is a next tuple
				Record temp;
				//cout << 1 << endl;
				//cout << "minLeftIndex " << minLeftIndex << endl;
				if (leftFiles[minLeftIndex]->GetNext(temp)) {
					//cout << "Inserting to Heap" << endl;
					leftHeap->insert(temp, minLeftIndex);
				}
				//cout << 2 << endl;
				// Extract new Min after possible insert, update index if extract works
				leftMin = leftHeap->extractMin(heapExtractLeft);
				//cout << 3 << endl;
				if (heapExtractLeft) {
					//cout << 4 << endl;
					minLeftIndex = leftMin->index;
					//cout << 5 << endl;
					//leftMin->data.print(cout, schemaLeft);
					//cout << endl;
					//cout << 6 << endl;

					// Compare current LeftMin with original LeftMin 
					//cout << 7 << endl;
					compResultTemp = leftComp.Run(tempLeft[0]->data, leftMin->data);
					//cout << 8 << endl;
					// If current LeftMin = original LeftMin
					// Push current LeftMin into tempLeft
					//cout << 9 << endl;
					if (compResultTemp == 0) {
						//cout << 10 << endl;
						tempLeft.push_back(leftMin);
					}
				}
				//cout << "Ending Loop" << endl;
				// Repeat until heap Tree has no node OR comparison is false
			}
			//cout << "Start Extracting Right " << endl;
			compResultTemp = 0;
			while (heapExtractRight && compResultTemp == 0) {
				//cout << "Extracting Right " << tempCounter++ << endl;
				minRightIndex = rightMin->index;
				// Pushing next tuple into right heap using minRightIndex
				// Only push IF there is a next tuple
				Record temp;
				//cout << 1 << endl;
				if (rightFiles[minRightIndex]->GetNext(temp)) {
					//cout << 2 << endl;
					rightHeap->insert(temp, minRightIndex);
				}
				//cout << 3 << endl;
				// Extract new Min after possible insert, update index if extract works
				rightMin = rightHeap->extractMin(heapExtractRight);
				//cout << 4 << endl;
				if (heapExtractRight) {
					//cout << 5 << endl;
					minRightIndex = rightMin->index;
					//cout << 6 << endl;
					//rightMin->data.print(cout, schemaRight);
					//cout << endl;

					// Compare current RightMin with original RightMin
					//cout << 7 << endl;
					compResultTemp = rightComp.Run(tempRight[0]->data, rightMin->data);
					// If current RightMin = original RightMin
					// Push current RightMin into tempRight
					//cout << 8 << endl;
					if (compResultTemp == 0) {
						//cout << 9 << endl;
						tempRight.push_back(rightMin);
					}
				}
				//cout << "End of Loop" << endl;
			}

			//cout << "Merging " << tempLeft.size() << " Left with " << tempRight.size() << " Right" << endl;

			// Printing everything in Left & Right
			//cout << "Size of Temp Left " << tempLeft.size() << endl;
			//tempLeft[0]->data.print(cout, schemaLeft);
			//cout << endl;

			//cout << "Size of Temp Right " << tempRight.size() << endl;
			//tempRight[0]->data.print(cout, schemaRight);
			//cout << endl;
			

			// Create all merge combinations
			for (int i = 0; i < tempLeft.size(); ++i) {
				for (int j = 0; j < tempRight.size(); ++j) {
					merge = new Record()	;
					merge->AppendRecords(tempLeft[i]->data, tempRight[j]->data, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());

					myOutput.AppendRecord(*merge);

					//appendRecords.push_back(merge);
				}
			}

			// Delete all HeapNode in tempLeft and tempRight
			for (int i = 0; i < tempLeft.size(); ++i) {
				delete tempLeft[i];
			}
			for (int j = 0; j < tempRight.size(); ++j) {
				delete tempRight[j];
			}

		}
		// Condition where leftMin > rightMin
		else if (compResult == 1)
		{
			//cout << "Left Greater Right" << "LR counter " << LRcounter++ << endl;
			//minRightIndex = rightMin->index;
			delete rightMin;
			// Pushing next tuple into right heap using minRightIndex
			// Only push IF there is a next tuple
			Record temp;
			if (rightFiles[minRightIndex]->GetNext(temp)) {
				//cout << "Found Get Next " << endl;
				rightHeap->insert(temp, minRightIndex);
			}
			rightMin = rightHeap->extractMin(heapExtractRight);
			if (heapExtractRight) minRightIndex = rightMin->index;
		}
		// Condition where leftMin < rightMin
		else if (compResult == -1) {
			//cout << "Left Smaller Right " << "RL counter " << RLcounter++ << endl;
			//minLeftIndex = leftMin->index;
			delete leftMin;
			// Pushing next tuple into left heap using minLeftIndex
			// Only push IF there is a next tuple
			Record temp;
			if (leftFiles[minLeftIndex]->GetNext(temp)) {
				//cout << "Found Get Next " << endl;
				leftHeap->insert(temp, minLeftIndex);
			}
			leftMin = leftHeap->extractMin(heapExtractLeft);
			if (heapExtractLeft) minLeftIndex = leftMin->index;
		}
		//cout << "End of while Loop" << endl;
		//cout << "extractLeft: " << heapExtractLeft << " extractRight: " << heapExtractRight << endl;
	}
	/*
	cout << "it works" << endl;
	if (leftHeap->getHeapSize() == 0 || rightHeap->getHeapSize() == 0) {
		finishMerge = true;
		for (int i = 0; i < leftFiles.size(); ++i) {
			leftFiles[i]->Close();
			delete leftFiles[i];
		}
		leftFiles.clear();
		for (int i = 0; i < rightFiles.size(); ++i) {
			rightFiles[i]->Close();
			delete rightFiles[i];
		}
		rightFiles.clear();
	}
	cout << "Printing Append Records" << endl;
	cout << appendRecords.size() << endl;
	for (int i = 0; i < appendRecords.size(); ++i) {
		appendRecords[i]->print(cout, schemaOut);
		cout << endl;
	}
	*/

	//exit(0);
	
	if (leftHeap->getHeapSize() == 0 || rightHeap->getHeapSize() == 0) {

		myOutput.AppendLast();

		myOutput.Close();


		appendedRecords.Open(myOutputFileC);


		finishMerge = true;
		for (int i = 0; i < leftFiles.size(); ++i) {
			leftFiles[i]->Close();
			delete leftFiles[i];
		}
		leftFiles.clear();
		for (int i = 0; i < rightFiles.size(); ++i) {
			rightFiles[i]->Close();
			delete rightFiles[i];
		}
		rightFiles.clear();

	}
	/*
				ostringstream oss;
			string result;
			oss << startLoc << right << i << bin;
			result = oss.str();
			cout << result << endl;
			fileNameC = new char[result.length() + 1];
			strcpy(fileNameC, result.c_str());
			tempDB = new DBFile();
			tempDB->Open(fileNameC);
			rightFiles.push_back(tempDB);
	*/
	for (int i = 0; i < leftFileNum; ++i) {
		ostringstream oss;
		string result;
		oss << startLoc << left << joinCount << i << bin;
		result = oss.str();
		remove(result.c_str());
	}
	for (int i = 0; i < rightFileNum; ++i) {
		ostringstream oss;
		string result;
		oss << startLoc << right << joinCount << i << bin;
		result = oss.str();
		remove(result.c_str());
	}

	cout << "Done with Merge" << endl;
	
}

Schema & Join::getSchema(){
	return schemaOut;
}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN\nLeft Schema:\n" << schemaLeft << "\nRight Schema:\n" << schemaRight << "\nSchema Out:\n" << schemaOut << "\nPredicate:\n" << predicate << "\nLeft Operator:\n{\n" << *left << "\n}\nRight Operator:\n{\n" << *right << "\n}" << endl;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer, OrderMaker &_duplComp) {
	schema = _schema;
	producer = _producer;
	check = true;
	duplComp.Swap(_duplComp);
}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT \nSchema: " << schema << "\nProducer: " << *producer << endl;
}

bool DuplicateRemoval::GetNext(Record& _record)//compiles but is not finished
{
	if (check)
	{
		Record recTemp;
		producer->GetNext(recTemp);
		recTemp.SetOrderMaker(&duplComp);
		KeyInt AMARSUCKSCOCK = KeyInt(1);
		duplTemp.Insert(recTemp, AMARSUCKSCOCK);
		check = false;
		Record recTemp2;
		while (producer->GetNext(recTemp2))
		{
			AMARSUCKSCOCK = KeyInt(1);
			recTemp2.SetOrderMaker(&duplComp);
			if (!duplTemp.IsThere(recTemp2))
			{
				duplTemp.Insert(recTemp2,AMARSUCKSCOCK);
			}
		}
		duplTemp.MoveToStart();
	}
	if (duplTemp.AtEnd())
	{
		return false;
	}
	else
	{
		_record=duplTemp.CurrentKey();
		duplTemp.Advance();
		return true;
	}
	
}

// Slow Variable to String Conversion Method
// Alternative Options (may require downloading):
// http://stackoverflow.com/questions/191757/how-to-concatenate-a-stdstring-and-an-int
template <typename T>
string convert(T x)
{
	ostringstream convert;   			// stream used for the conversion
	convert << x;		      			// insert the textual representation of 'Number' in the characters in the stream
	return convert.str(); 				// set 'Result' to the contents of the stream
}

Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
	first = true;
}

Sum::~Sum() {
	
}

// TEST WITH:
//		- Queries/Phase4Queries/1.sql, 2, 5, 9, 11-16
bool Sum::GetNext(Record & _record)
{
	Record temp;
	int valI; double valD;
	double runningSum = 0;
	
	if (first) {
		while (producer->GetNext(temp)) {

			compute.Apply(temp, valI, valD);

			if (compute.GetSumType() == 1) {		// int 
				runningSum += valI;
			}
			else if (compute.GetSumType() == 0) {	// double
				runningSum += valD;
			}
		}
		first = false;
	}
	else {
		return false;
	}

	// create recrod with running sum
	// Create bits
	double doubletemp = runningSum;
	// size = (size of bits) + (location of double) + (size of double)
	int bitsize = sizeof(int) + sizeof(int) + sizeof(double);
	char* bits = new char[bitsize];								// temp storage
																// columnLoc = (size of bits) + (location of double)
	int columnLoc = sizeof(int) + sizeof(int);					// column location
	*((double *) &(bits[columnLoc])) = doubletemp;				// insert sum
	((int *)bits)[0] = bitsize;									// size of bits
	((int *)bits)[1] = columnLoc;								// location of the sum

	_record.Consume(bits);										// Define _record (SUM Record)
	delete bits;												// Delete bits

	/*
	FILE* fp;
	string s;
	string separator = convert('|');

	s = convert(runningSum) + separator;
	char* str = new char[s.length() + 1];			// string to char* converter
	strcpy(str, s.c_str());
	fp = fmemopen(str, s.length() * sizeof(char), "r");

	//Extract the "FILE"
	vector<int> x;
	x.push_back(0);
	Schema sumz = schemaOut;
	sumz.Project(x);
	_record.ExtractNextRecord(sumz, *fp);
	*/
	return true;
}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM\nSchemaIn: " << schemaIn << "\nSchemaOut: " << schemaOut << " Function" << "Producer: " << *producer << endl;//told by TA to just use "Function"
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts.Swap(_groupingAtts);
	compute = _compute;
	producer = _producer;
	first = true;
	counter = 0;
}


// NOW SUPPORTS ALL POSSIBLE GROUPING ATTRIBUTES AND SUMS
// KEY = Record(GROUPING ATTRIBUTE); DATA = SUM
// TEST WITH:
//		- Queries/Phase4Queries/11.sql for <int, float>
//		- Queries/Phase4Queries/12.sql for <string, float>
//		- Queries/Phase4Queries/13.sql for <float, float>
//		- Queries/Phase4Queries/14.sql for <int, int>
//		- Queries/Phase4Queries/15.sql for <string, int>
//		- Queries/Phase4Queries/16.sql for <float, int>
bool GroupBy::GetNext(Record& _record) {
	
	if (first)																	// check if this is called the first time
	{
		Record temp, projtemp;													// Records to insert
		int intResult = 0; double doubleResult = 0;										// for Function's Apply (computing sum)
		int returnsInt;															// to determine whether the sum type is int
		double sum;																// SUM to insert
		KeyDouble td;															// temp KeyDouble
		while (producer->GetNext(temp))
		{
			returnsInt = compute.Apply(temp, intResult, doubleResult);			// compute sum
			sum = intResult + doubleResult;										// get sum
			projtemp = temp;													// extract only grouping attributes from Record
			projtemp.SetOrderMaker(&groupingAtts);
			td = KeyDouble(sum);
			if (map.IsThere(projtemp))											// the map has the record
				map.Find(projtemp) = KeyDouble(map.Find(projtemp) + sum);
			else																// the map doesn't have the record
				map.Insert(projtemp, td);										// insert into map
			intResult = 0; doubleResult = 0;
		}
		first = false;															// set first time to false
		map.MoveToStart();
	}
	
	if (map.AtEnd())
		return false;
	Record r1, r2;
	char* c;
	int size;

	// Create a FILE here
	/*FILE* fp;
	string s;
	string separator = convert('|');
	double doubletemp = map.CurrentData();
	s = convert(doubletemp) + separator;
	char* str = new char[s.length() + 1];					// string to char* converter
	strcpy(str, s.c_str());
	fp = fmemopen(str, s.length() * sizeof(char), "r");

	//Extract the "FILE"
	
	vector<int> x;
	x.push_back(0);
	Schema sumz = schemaOut;
	sumz.Project(x);
	r1.ExtractNextRecord(sumz, *fp);
	*/

	// Create bits
	double doubletemp = map.CurrentData();
	// size = (size of bits) + (location of double) + (size of double)
	int bitsize = sizeof(int) + sizeof(int) + sizeof(double);
	char* bits = new char[bitsize];								// temp storage
	// columnLoc = (size of bits) + (location of double)
	int columnLoc = sizeof(int) + sizeof(int);					// column location
	*((double *) &(bits[columnLoc])) = doubletemp;				// insert sum
	((int *)bits)[0] = bitsize;									// size of bits
	((int *)bits)[1] = columnLoc;								// location of the sum
	
	r1.Consume(bits);											// Define r1 (SUM Record)
	delete bits;												// Delete bits

	// Create r2 (GroupingAtts)
	map.CurrentKey().Project(groupingAtts.whichAtts, groupingAtts.numAtts, schemaIn.GetNumAtts());
	c = map.CurrentKey().GetBits();
	size = map.CurrentKey().GetSize();
	r2.CopyBits(c, size);
	
	_record.AppendRecords(r1, r2, 1, groupingAtts.numAtts);		// Merge 2 records

	//fclose(fp);
	//delete str;
	//delete c;
	map.Advance();
	return true;
}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY\nSchemaIn: " << schemaIn << "\nSchemaOut: " << schemaOut << "\nGroupingAtts: " << groupingAtts << "\nFunction:\nFunction\n" << "Producer:\n{\n" << *producer << "\n}" << endl;//told by TA to just use "Function"
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	s = _schema;
	outFile = _outFile;
	producer = _producer;
	counter = 0;
}

WriteOut::~WriteOut() {

}

bool WriteOut::GetNext(Record& _record) {
	//ofstream myOutputFile;								// Unn, no difference
	//myOutputFile.open(outFile.c_str());					// Unn, no difference

	// Delete Me
	//cout << "Write Out Counter: " << counter++ << endl;
	if (producer->GetNext(_record)) {
		_record.print(cout, schema);
		cout << endl;	
		//myOutputFile.close();								// Unn, no difference

		return true;
	}

	//myOutputFile.close();									// Unn, no difference
	return false;
}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT\nSchema: " << schema << "\nOut File: " << outFile << "\nProducer:\n{\n" << *producer << "\n}\n";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE" << "(\n" << *_op.root << "\n)";
}
