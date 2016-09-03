package dbs_project.persistence.impl;
import java.io.*;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import dbs_project.query.impl.QueryLayerImpl;
import dbs_project.exceptions.NoTransactionActiveException;
import dbs_project.exceptions.QueryExecutionException;
import dbs_project.persistence.PersistenceLayerExtend;
import dbs_project.exceptions.TransactionAlreadyActiveException;
import dbs_project.storage.Relation;
import dbs_project.index.IndexableTable;
import dbs_project.query.statement.*;

/**
 * Created by Xedos2308 on 16.02.15.
 */
public class PersistenceLayerImpl extends QueryLayerImpl implements PersistenceLayerExtend {


	private boolean persistanceEnabled;
	private boolean isTransactionRunning;

	//persistence is setted
	@Override
	public void setPersistence(boolean enabled) {

		persistanceEnabled = enabled;

	}

	//return status of Persistence
	public boolean getPersistence() {

		return persistanceEnabled;
	}

	// check, whether an Transaction is already running,
	// if not set the transaction on true
	@Override
	public void beginTransaction() throws TransactionAlreadyActiveException {

		if(isTransactionRunning)
		{
			throw new TransactionAlreadyActiveException();
		}

		isTransactionRunning = true;

	}

	//commit transaction
	@Override
	public void commitTransaction() throws NoTransactionActiveException {

		//check whether an active transaction exists
		if(!isTransactionRunning)

		{
			throw new NoTransactionActiveException();
		}

		//is needed to create a list of modified tables
		StringBuffer tableNamesList = new StringBuffer();

		try {

			for (IndexableTable table : this.storageLayer.getIndexableTables()) {
				//create a new File for one special table, "rw" means support reads and writes
				RandomAccessFile raFile = new RandomAccessFile(table.getTableMetaData().getName()+".ser", "rw");

                // is a bytes stream class that’s used to handle raw binary data.
                // To write the data to file, you have to convert the data into bytes and save it to file
				FileOutputStream fileOutStream = new FileOutputStream(raFile.getFD());

				//provides buffering to your output streams. Buffering can speed up IO quite a bit.
				//Rather than write one byte at a time to the network or disk, you write a larger block at a time.
				//This is typically much faster, especially for disk access and larger data amounts.
				BufferedOutputStream bufferOutStream = new BufferedOutputStream(fileOutStream);

				// Persistent storage of objects can be accomplished by using a file for the stream Only objects
				// that support the java.io.Serializable interface can be written to streams.
				// The class of each serializable object is encoded including the class name and signature of the class,
				// the values of the object's fields and arrays, and the closure of any other objects referenced from the initial objects.
				ObjectOutputStream objOutStream = new ObjectOutputStream(bufferOutStream);

				// is used to write an object to the stream. Any object, including Strings and arrays, in our case table
				objOutStream.writeObject(table);

				//flushes the stream
				objOutStream.flush();

				//closes the stream
				objOutStream.close();

				//closes the fileOutputStream
				fileOutStream.close();

				// put the used table to the modified tables
				tableNamesList.append(table.getTableMetaData().getName() + "\n");

				//close the Buffering Stream
				bufferOutStream.close();

				//close the randomAccessFile
				raFile.close();

			}

			//writes a String to a file creating the file if it does not exist using the default encoding
			//tablesList.lst is the file to write, tableNameslist is the content
			FileUtils.writeStringToFile(new File("TablesList.lst"), tableNamesList.toString());

		}
		catch (FileNotFoundException e) {

			e.printStackTrace();

		}
		catch (IOException e) {

			e.printStackTrace();

		}

		//transaction is done
		//all files have been commited
		isTransactionRunning =false;
	}

	//
	@Override
	public void abortTransaction() throws NoTransactionActiveException {

		//check whether the transaction is anyway running
		if(!isTransactionRunning){

			throw new NoTransactionActiveException();

		}
		// just abort the transaction without any commit-calls!
		isTransactionRunning = false;

		//try to restore the state written to stable
		//storage.Try to perform crash recovery if necessary.
		try {
			startUp();

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	//indicates if there is an uncommited transaction
	@Override
	public boolean hasActiveTransaction() {

		return isTransactionRunning;
	}

	// Start the Database and restore the state written to stable
	// storage. Perform crash recovery if necessary.
	@Override
	public void startUp() {

		// our TableHashMap defined in the TableImpl
		HashMap<Integer, IndexableTable> tableHashMap = new HashMap<>();

		//create a new File with the TableNames
		File file = new File("TablesList.lst");

		//check it out whether the file is existing
		try {
			if(file.exists()){

				// create a BufferReader to read the Buffer from the file
				BufferedReader buffRead =new BufferedReader(new FileReader(file));

				// needed to decide where we read from
				String lineToRead;

				//the while condition allows us to read one line at a time from the file
				while ((lineToRead = buffRead.readLine()) != null) {

                    //the opposite of the FileoutPutstream, we load the line to read
					FileInputStream fileInStream = new FileInputStream(lineToRead+".ser");

					//"" see after in commit-method
					BufferedInputStream buffInStream = new BufferedInputStream(fileInStream);

                    // "" see after in commit-method
					ObjectInputStream objInStream = new ObjectInputStream(buffInStream);

                    // read the current table from the streamline one at a time
					IndexableTable table=(IndexableTable)objInStream.readObject();

					// put the table to update in the HashMap
					tableHashMap.put(table.getTableMetaData().getId(),table);

					// close the streams
					objInStream.close();
					fileInStream.close();
				}
				buffRead.close();

				// send the HashMap with the Updatetables to storageLayer
				// and use them there
				storageLayer.restoreTablesFromTheFile(tableHashMap);

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}


	}

	/**
	 * Need to override all the following methods cause of the QueryLayerImpl.
	 **/

	@Override
	public void createTable(CreateTableStatement createTableSt)
			throws QueryExecutionException {
		//check whether transaction is running
		try{
			if(!isTransactionRunning)
			{
				//if not start transaction
				beginTransaction();
				//create a table with given Statement
				super.createTable(createTableSt);
				// commit
				commitTransaction();
				return;
			}

			else

			{
				//use the current transaction and create a table
				createTable(createTableSt);
			}
		}catch(TransactionAlreadyActiveException e){
			e.printStackTrace();
		} catch (NoTransactionActiveException e) {
			e.printStackTrace();
		}

	}


	@Override
	public void dropTable(DropTableStatement dropTableSt)
			throws QueryExecutionException {
		//same here
		try{
			if(!isTransactionRunning){

				beginTransaction();
				//drop the table
				super.dropTable(dropTableSt);
				//commit
				commitTransaction();
				return;

			}else{
				// still drop in the current transaction
				super.dropTable(dropTableSt);
			}
		}catch(TransactionAlreadyActiveException e){
			e.printStackTrace();
		} catch (NoTransactionActiveException e) {
			e.printStackTrace();
		}

	}
	@Override
	public void createColumn(CreateColumnStatement createColSt)
			throws QueryExecutionException {
		//testing
		try{
			if(!isTransactionRunning){

				//everything is fine here
				beginTransaction();
				super.createColumn(createColSt);
				commitTransaction();
				return;

			}else{
				//create this column
				super.createColumn(createColSt);
			}
		}catch(TransactionAlreadyActiveException e){
			e.printStackTrace();
		} catch (NoTransactionActiveException e) {
			e.printStackTrace();
		}

	}
	@Override
	public void dropColumn(DropColumnStatement dropColSt)
			throws QueryExecutionException {
		//testing
		try{
			if(!isTransactionRunning){

				//still fine nothing new happens
				beginTransaction();
				super.dropColumn(dropColSt);
				commitTransaction();
				return;
			}else{
				//the column is dropped
				super.dropColumn(dropColSt);
			}
		}catch(TransactionAlreadyActiveException e){
			e.printStackTrace();
		} catch (NoTransactionActiveException e) {
			e.printStackTrace();
		}

	}
	@Override
	public int executeDeleteRows(DeleteRowsStatement deleteSt)
			throws QueryExecutionException {

		//how many rows have been executed
		int rowCounter = 0;
		try{
			//still testing
			if(!isTransactionRunning){

				beginTransaction();
				rowCounter = super.executeDeleteRows(deleteSt);
				commitTransaction();
				return rowCounter;
			}else{
				//execute on running transaction
				rowCounter=super.executeDeleteRows(deleteSt);
			}
		}catch(TransactionAlreadyActiveException e){
			e.printStackTrace();

		} catch (NoTransactionActiveException e) {
			e.printStackTrace();
		}
		return rowCounter;

	}


	@Override
	public void executeInsertRows(InsertRowsStatement insertSt)
			throws QueryExecutionException {
		//testing
		try{
			if(!isTransactionRunning){

				//execute the inserted Rows
				beginTransaction();
				super.executeInsertRows(insertSt);
				commitTransaction();
				return;
			}else{
				//ok
				super.executeInsertRows(insertSt);
			}
		}catch(TransactionAlreadyActiveException e){
			e.printStackTrace();
		} catch (NoTransactionActiveException e) {
			e.printStackTrace();
		}

	}

	@Override
	public int executeUpdateRows(UpdateRowsStatement updateSt)
			throws QueryExecutionException {

		int rowCounter = 0;
		try{
			if(!isTransactionRunning){

				// like for deleated Rows same procedure here
				beginTransaction();
				rowCounter=super.executeUpdateRows(updateSt);
				commitTransaction();

				//return the Number of Rows executed
				return rowCounter;
			}else{
				rowCounter=super.executeUpdateRows(updateSt);
			}
		}catch(TransactionAlreadyActiveException e){
			e.printStackTrace();
		} catch (NoTransactionActiveException e) {
			e.printStackTrace();
		}
		return rowCounter;
	}
	@Override
	public Relation executeQuery(QueryStatement querySt)
			throws QueryExecutionException {

		// Relation have to be returned
		Relation rel = null;
		try{
			if(!isTransactionRunning){

				//execute the Query and return the results in form of a new relation
				beginTransaction();
				rel=super.executeQuery(querySt);
				commitTransaction();
				return rel;
			}else{
				//""
				rel=super.executeQuery(querySt);
			}

		}catch(TransactionAlreadyActiveException e){
			e.printStackTrace();
		} catch (NoTransactionActiveException e) {
			e.printStackTrace();
		}
		return rel;

	}

	// we want to empty the Log File
	@Override
	public void deleteLogFiles() {

		// our File with tableNames
		File file = new File("TablesList.lst");
		try {
			// testing
			if(file.exists()){

				// create a Buff Reader to read the Buffer from the given File
				BufferedReader buffRead = new BufferedReader(new FileReader(file));
				//the line
				String lineToRead;
				//""
				while ((lineToRead = buffRead.readLine()) != null) {
					// delete the Tables one at a time
					File tableFile=new File(lineToRead+".ser");
					tableFile.delete();
				}
				//close the streams and file
				buffRead.close();
				file.delete();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void shutDown() {

		try {
			//if an Active transaction exists
			if(hasActiveTransaction())

				//commit this and shut down
				commitTransaction();

		} catch (NoTransactionActiveException e)
		{
			e.printStackTrace();
		}

	}

}
