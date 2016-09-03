#DBMS
Main Memory DBMS was our project of the core course Database Systems at Saarland University to deepen the understanding about the internals of databases. For each milestone, skeleton containing some interfaces were provided that had to be implemented, together with detailed information on the tasks to accomplish and a set of functional and performance tests.
##Tools and Project Setup
You need the following tools for the project: <br />
• Oracle Java SE Development Kit (JDK), Version 7 or higher <br />
• Maven: http://maven.apache.org <br />
One of the following IDEs: <br />
• Netbeans: http://www.netbeans.org/ (out of the box Git and Maven support) <br />
• Intellij Idea CE: http://www.jetbrains.com/idea/download/ <br />
• Eclipse: http://www.eclipse.org/ (plugins for Git and Maven available) <br />
The project can be built using Maven, with the command mvn compile. Maven can also create a project suitable for Eclipse with the command: mvn eclipse:eclipse. For more details see the documentation on Maven. 
##TASK 1: IMPLEMENTATION OF STORAGE LAYER
For the first milestone, we had to implement the storage layer of our database. We were provided with the interfaces that were to be implemented and against which there were tests tests (for Task 1, most of the interfaces needed are located in the storage package). The resources contained in the following packages (located under src/main/java/dbs_project/) are relevant for Task 1:
• exceptions: exception definitions. <br />
• database: the interface Database is the starting point for tests. The implementation constructs and connects the different layers from all tasks in the end. For the first task, we only have to implement the getStorageLayer() method. <br />
• storage: this package contains the main interfaces to be implemented. Here we have the following classes and interfaces:
– StorageLayer: represents the storage layer of our database. We had to provide our own implementation of this interface, for example in storage/impl in a class called StorageLayerImpl. <br />
– Table: represents a relational table in our database. <br />
– Column: represents a column in a relational table (which is one possible view of the data in a table). <br />
– Row: represents a row in a database table (which is one possible view of the data in a table). <br />
– [Table|Column|Row]MetaData: meta information and properties for tables, columns and rows. <br />
– [Column|Row]Cursor: cursors to iterate over columns and rows. <br />
– Type: provides information about the supported data types. <br />
• util: contains some utility classes and general interfaces 
##TASK 2: IMPLEMENTATION OF INDEXING LAYER
For the second task, we had to implement the indexing layer of our database, which sits on top of the storage layer we implemented in the first task. The following interfaces and classes are important for this task: <br />
• index/IndexLayer: The IndexLayer interface represents the indexing layer of the database. Logically, it sits on top of the storage layer to provide additional functionality for more efficient access. The IndexLayer is designed as a decorator to the StorageLayer and contains the same methods (to delegate to the StorageLayer). <br />
• index/Index: Interface for an index on one column of a table. Indexes reflect the data in the column and any changes to that data. The implementation had to synchronize with the column somehow on inserts, updates, deletes, etc. Since the given interface only describes how to query the index, we had to introduce own methods in our implementation for inserts, deletes or updates. We had to implement hash and tree index (see IndexType enum). <br />
• index/IndexableTable: This interface represents a table that can create indexes on its columns. It is designed as a decorator to Table and contains the same methods and some additional functions for indexes. <br />
• index/IndexMetaInformation: This interface delivers meta information about an index (like index type, cardinality, ... ). <br />
• index/IndexType: Enum for the possible index types: <br />
– HASH : hash based index for point queries (no range support) <br />
– TREE : tree based index for point and range queries 
##TASK 3: IMPLEMENTATION OF QUERY LAYER 
For the third task, we had to implement the query layer of our database. The query layer sits on top of the storage and index layer and is a high level interface that is very close to the user. Imagine this layer to sit behind a SQL parser that takes the user input, creates the different statement objects and calls the query layer methods. As a consequence, value literals are passed to this layer as strings and have to be parsed to different types by the system. The query layer is the top level facade to the database functionality and provides the different methods for executing DML statements, DDL statements and queries. Behind this facade, the code had to (or in some cases can) do things like selecting rows by evaluating predicates against the data, join processing, query optimization, index creation/selection. The following interfaces and packages are relevant for this task: <br />
• query/QueryLayer: the query layer interface, which provides facade methods for DML, DDL and queries. <br />
• query/statement/: this package contains all types of statement arguments for the query layer methods. <br />
• query/predicate/: this package contains the interfaces for predicates that are used in statements (WHERE clause). You can use a visitor pattern to translate them into whatever form your system needs for evaluation. The package also contains a small example. 
##TASK 4: TRANSACTIONS 
For the final task, we had to implement a simple persistence layer for our database. This layer allows to write database to stable storage and restore the state of the database from there. It supports a simple form of transactions, with only one single active transaction at a time. The properties to be implemented were atomicity and durability: <br />
• Every change by a committed transaction has to be durable. <br />
• If an active transaction is interrupted (e.g. crash, power failure, ...) before committing, all changes from that transaction must not reflect in the database after recovery. <br />
• When the database is started, it has to perform crash recovery if necessary. <br />
For the interface Database we had to implement the following methods: <br />
• void startUp() throws IOException : Starts the Database and restore the state written to stable storage. Perform crash recovery if necessary. <br />
• void shutDown() throws IOException : Persist the state of the database to stable storage. <br />
Additionally, following methods from interface PersistenceLayer had to be implemented: <br />
• void beginTransaction() throws TransactionAlreadyActiveException : Begins new transaction. System only handles one transaction at a time and throws an exception if another transaction is still active. <br />
• void commitTransaction() throws NoTransactionActiveException : End the active transaction and make it durable. Exception if there is no active transaction. <br />
• void abortTransaction() throws NoTransactionActiveException : Revert all changes from the active transaction and end it. Exception if there is no active transaction. <br />
• boolean hasActiveTransaction() : Indicates if there is an uncommitted transaction.
