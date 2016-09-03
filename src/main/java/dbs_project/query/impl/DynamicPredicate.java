package dbs_project.query.impl;

/**
 * Created by Xedos2308 on 12.01.15.
 */
public interface DynamicPredicate {

	boolean eval(int rowId);

}
