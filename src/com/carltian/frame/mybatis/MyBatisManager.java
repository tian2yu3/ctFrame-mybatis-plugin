package com.carltian.frame.mybatis;

import java.util.List;

import com.carltian.frame.db.DatabaseManager;

public interface MyBatisManager extends DatabaseManager {

	public abstract void openSharedSession();

	public abstract void closeSharedSession();

	public abstract <T> T getMapper(Class<T> clazz);

	public abstract Transaction getTransaction();

	public abstract Transaction getTransaction(boolean autoClose);

	public abstract <T> T selectOne(String sqlmap);

	public abstract <T> T selectOne(String sqlmap, Object parameter);

	public abstract <E> List<E> selectList(String sqlmap);

	public abstract <E> List<E> selectList(String sqlmap, Object parameter);

	public abstract int insert(String sqlmap);

	public abstract int insert(String sqlmap, Object parameter);

	public abstract int update(String sqlmap);

	public abstract int update(String sqlmap, Object parameter);

	public abstract int delete(String sqlmap);

	public abstract int delete(String sqlmap, Object parameter);

}
