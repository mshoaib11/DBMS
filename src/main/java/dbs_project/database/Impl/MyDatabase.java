package dbs_project.database.Impl;

import java.io.IOException;
import dbs_project.database.Database;
import dbs_project.index.IndexLayer;
import dbs_project.persistence.PersistenceLayer;
import dbs_project.persistence.impl.PersistenceLayerImpl;
import dbs_project.query.QueryLayer;
import dbs_project.query.impl.QueryLayerImpl;
import dbs_project.storage.StorageLayer;
import dbs_project.storage.impl.StorageLayerImpl;


/**
 * Created by Xedos2308 on 05.11.14.
 */
public class MyDatabase implements Database {


    PersistenceLayerImpl persistence = null;

    @Override
    public PersistenceLayer getPersistenceLayer() {

        if(persistence==null)
        {
            persistence= new PersistenceLayerImpl();
        }
        return  persistence;
    }

    @Override
    public StorageLayer getStorageLayer() {
        StorageLayer newStorageLayer= new StorageLayerImpl();
        return newStorageLayer;
    }

    @Override
    public IndexLayer getIndexLayer() {
        IndexLayer indexLayer=new StorageLayerImpl();
        return indexLayer;
    }

    @Override
    public QueryLayer getQueryLayer() {
        if(persistence!=null&&persistence.getPersistence()){
            return persistence;
        }
        QueryLayer queryLayer=new QueryLayerImpl();
        return queryLayer;
    }

    @Override
    public void startUp() throws IOException {
        persistence.startUp();

    }

    @Override
    public void shutDown() throws IOException {
        persistence.shutDown();

    }

    @Override
    public void deleteDatabaseFiles() throws IOException {
        this.persistence.deleteLogFiles();

    }

}
