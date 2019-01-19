# Database Systems Implementation

A semester-long project that is a implementation of a single-user relational database system with a limited SQL-like language. The project is divided into 6 stages that build on top of each other, and follow the path of a SQL query from the user interface all the way down to the storage, and back through the relational algebra operators with the result displayed on the user’s screen.

The 6 stages of the project are:

1. Database Catalog
	- Implemented a database catalog, or metadata. The catalog contains data about the objects in the database, such as tables, attributes, views, indexes, and triggers.
	- The query parser checks that all the tables/attributes in a query exist in the database and that the attributes are used correctly in expressions.
	- The query optimizer extracts statistics on tables and attributes from the catalog in order to compute the optimal query execution plan.
	- The catalog in this project contains only data on tables and their corresponding attributes. For each table, the name, the number of tuples, and the location of the file containing the data are stored.
	- For each attribute, the name, type, and number of distinct values are stored.

2. Query Compiler
	- First, a query parser is used to transform the text of a SQL query into an abstract representation that extracts the defining elements required for the execution of the query.
	- Query Lexer was used for the query parser.
	- The query compiler takes as input the data structures produced by the parser and transforms them into a query execution tree of relational algebra operators.
	- Scan, Select, Project, Join, DuplicateRemoval (Distinct), Sum (Aggregates), and the GroupBy were all considered in the query execution tree.

3. Heap File
	- A heap file stores the records of a table. The records are grouped into pages, which represent the I/O unit.
	- Pages eliminate the requirement to access the file (disk) for every record. This improves the I/O bandwidth utilization.
	- Records are stored in arbitrary order in a heap file. The only access path to a heap file is to read sequentially all the records, from beginning to end.
	- A heap file is created for every table.
	- Implemented functions to create, open, close, move file pointers, get next record, append record, and load records for heap files.

4. Relational Algebra Operators
	- This project requires the implementation of the remaining four relational algebra operators (Join, DuplicateRemoval, Sum, and GroupBy) in the case when data fit in memory. 
	- In other words, we implemented one-pass versions of these operators. With these operators, it is possible to run all the SQL queries supported by our reduced query language, as long as each operator in the plan can be evaluated in memory.
	- Essentially, the output of this phase is an in-memory database server.

5. Sorted File and Sort-Based Relational Algebra Operators
	- Implemented a two-pass sort-merge join operator.
	- In the sort phase, the input relations are split into fragments that fit in the available memory.
	- Each fragment is sorted on the join attribute(s) and written to disk. A sorted fragment is also called a run.
	- In the merge phase, a page is read from each run across the two relations, the minimum value for the join attribute is extracted from each relation, and a join tuple is generated, if the minimum values are identical. If that is not the case, the minimum value is discarded.
	- Whenever a page from a run is emptied, a subsequent page from the same run is read in its place. The procedure finishes when all the runs from one of the two relations are exhausted.


6. B+- Tree Index
	- Implemented a B+- Tree for indexing.
	- The index properties (name, table, attribute, and the name/location of the index file) are stored in the catalog, to be used in query processing.