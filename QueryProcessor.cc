#include "ClientConfig.h"
#include "CommunicationFramework.h"
#include "QueryProcessor.h"
#include "ParseTree.h"
#include "ParseTreeFunctions.h"
#include "ProxyEventProcessor.h"
#include "DistTypes.h"
#include "Messages.h"
#include "ResultProcessor.h"
#include "Constants.h"
#include "Logging.h"

extern "C"{
#include "QueryParser.h"
}

#include <string>
#include <unistd.h>
#include <sstream>

using namespace std;


// this data structure holds the result of the parsing
extern struct SQLStatementStruct* sqlStatement;

typedef struct yy_buffer_state* YY_BUFFER_STATE;
extern "C" YY_BUFFER_STATE yy_scan_string(char*);
extern "C" void yy_delete_buffer(YY_BUFFER_STATE);

extern "C" int yyparse();
extern "C" int yylex_destroy();

extern int CLIENT_LISTEN_PORT;


QueryProcessor::QueryProcessor() :
	port(-1) {
}

QueryProcessor::~QueryProcessor() {
}


// connect to the server given by machine and port
bool QueryProcessor::Connect(string _serverName, int _serverPort) {
	/////////////////////////////////////////////////////////////////////////
	//start the communication framework
	port = CLIENT_LISTEN_PORT;
	StartCommunicationFrameworkFlexible(port);
	// output port can be different from the input

	/////////////////////////////////////////////////////////////////////////
	// connect to the server for SQL queries
	ProxyEventProcessor _sender;
	off_t sProtocol = SERVICE_SQL;
	NodeID server(_serverName, _serverPort);
	int ret = FindRemoteEventProcessor(server, sProtocol, _sender);
	if (ret != 0) {
		ostringstream ss;
		ss << "ERROR: Cannot connect to SQL server " << _serverName << ":"
		   << _serverPort << "!" << endl;
		ss << "Exit!" << endl;
		string s = ss.str();
		LOG_ENTRY_P("%s", s.c_str());

		//stop the communication framework
		StopCommunicationFramework();

		return false;
	}
	
	//send the message to the identified proxy (and from there to the coordinator)
	NodeID me(GetMachineName(), port);
	RegisterClientMessage_Factory(_sender, me);

	// remember the sender for future communication messages
	senderSQL.Swap(_sender);


	/////////////////////////////////////////////////////////////////////////
	// connect to the server for EXEC PLANS
	sProtocol = SERVICE_QUERY;
	ret = FindRemoteEventProcessor(server, sProtocol, _sender);
	if (ret != 0) {
		ostringstream ss;
		ss << "ERROR: Cannot connect to EXEC PLAN server " << _serverName << ":"
		   << _serverPort << "!" << endl;
		ss << "Exit!" << endl;
		string s = ss.str();
		LOG_ENTRY_P("%s", s.c_str());

		//stop the communication framework
		StopCommunicationFramework();

		return false;
	}

	// remember the sender for future communication messages
	senderExecPlan.Swap(_sender);

	return true;
}

// connect to the server given by machine and port
void QueryProcessor::Disconnect() {
	//unregister from the coordinator machine specified in the configuration
	NodeID me(GetMachineName(), port);
	UnregisterClientMessage_Factory(senderSQL, me);
	sleep(1);

	//stop the communication framework
	StopCommunicationFramework();
}


// execute statement without the need to return results to the caller
// the result is printed on the screen
// for queries, only a limited number of results are printed
// there is interaction required to print all the results
bool QueryProcessor::ExecuteBlind(string _stmt) {
	//////////////////////////////////////////////////////////
	// do a local syntactic verification of the SQL statement
	// not really required
	char* stmt = const_cast<char*>(_stmt.c_str());
	if (false == parse(stmt)) return false;

	//////////////////////////////////////////////////////////
	// at this point we have the parse tree in the ParseTree data structures
	if (sqlStatement->type == YY_SQL_QUERY) {
		// query statement that expects a table result to be returned
	}
	else {
		// statement does not generate a table result
		// create a result processor capable of processing the result
		ResultProcessor resProc;
		RegisterAsRemoteEventProcessor(resProc, SERVICE_SQL_RESPONSE);
		resProc.ForkAndSpin();

		// send the SQL statement to the query processor
		NodeID me(GetMachineName(), port);
		SQLNoResultQueryMessage_Factory(senderSQL, _stmt, me);

		ostringstream ss;
		ss << "Statement: {" << _stmt << "} submitted." << endl;
		string s = ss.str();
		LOG_ENTRY_P("%s", s.c_str());

		// block until the result is returned and processed
		resProc.Join();
		UnregisterRemoteEventProcessor(SERVICE_SQL_RESPONSE);
	}

	//////////////////////////////////////////////////////////
	// the query execution tree is built
	// we can delete the parser data structures
	Free_SQLStatementStruct(sqlStatement);
}

// execute statement (i.e., query) that returns results
bool QueryProcessor::ExecuteQuery(string _query) {
	return true;
}

// execute statement that does not return results
bool QueryProcessor::ExecuteStatement(string _stmt) {
	return true;
}

// execute execution plan that returns a GLA
bool QueryProcessor::ExecuteGLAPlan(string _graph, string _waypoint) {
	//send the execution plan to the coordinator
	NewJobMessage_Factory(senderExecPlan, _graph, _waypoint);

	return true;
}


// parse the statement and generate the parse tree
bool QueryProcessor::parse(char* _stmt) {
	// the query parser is accessed directly through yyparse
	// this populates the extern data structures
    YY_BUFFER_STATE buffer = yy_scan_string(_stmt);

    int parse = -1;
	if (yyparse () == 0) {
		LOG_ENTRY("Syntax check: OK!");
		parse = 0;
	}
	else {
		LOG_ENTRY("Syntax check: FAIL!");
		parse = -1;
	}

	yy_delete_buffer(buffer);
	yylex_destroy();

	if (parse != 0) return false;

	return true;
}
