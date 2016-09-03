package dbs_project.util;

import java.io.IOException;

public class OneIntIteratorWrapper implements IdCursor {
	
	private boolean next = true;
	private final int id;
	
	private OneIntIteratorWrapper(int id) {
		this.id = id;
	}

	@Override
	public boolean next() {
		if(next) {
			next = false;
			return true;
		}
		
		return false;
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public int getId() {
		return id;
	}
	
	public static OneIntIteratorWrapper wrap(int id) {
		return new OneIntIteratorWrapper(id);
	}

}
