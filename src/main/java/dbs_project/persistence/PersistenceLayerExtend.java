package dbs_project.persistence;

/**
 * Created by Xedos2308 on 16.02.15.
 */
public interface PersistenceLayerExtend extends PersistenceLayer {


	public void startUp();
	public void deleteLogFiles();
}
