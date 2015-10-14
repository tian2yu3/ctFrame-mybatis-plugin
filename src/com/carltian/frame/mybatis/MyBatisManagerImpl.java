package com.carltian.frame.mybatis;

import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import com.carltian.frame.CurrentContext;
import com.carltian.frame.container.ContainerImpl;
import com.carltian.frame.container.annotation.ContainerConstructor;
import com.carltian.frame.container.annotation.InitArg;
import com.carltian.frame.container.annotation.Resource;
import com.carltian.frame.mybatis.annotation.Mapper;
import com.carltian.frame.util.FrameLogger;

/**
 * 作用：提供MyBatis持久层通用访问方法 <br>
 * 背景：无 <br>
 * 备注：无 <br>
 * 变更：Carl Tian 2013-01-24<br>
 */
/**
 * @author Administrator
 * 
 */
public class MyBatisManagerImpl implements MyBatisManager {
	static public final String DEFAULT_CONFIG_PATH = "/WEB-INF/mybatis.xml";

	private final SqlSessionFactory sqlSessionFactory;
	private final ThreadLocal<SqlSession> sharedSession = new ThreadLocal<SqlSession>();

	@ContainerConstructor
	public MyBatisManagerImpl(@InitArg("config") String configPath, @Resource ContainerImpl container) {
		if (configPath == null || "".equals(configPath)) {
			configPath = DEFAULT_CONFIG_PATH;
		}
		InputStream is = CurrentContext.getServletContext().getResourceAsStream(configPath);
		sqlSessionFactory = (is == null) ? null : new SqlSessionFactoryBuilder().build(is);
		if (sqlSessionFactory != null) {
			// 注册Mapper
			Collection<Class<?>> mappers = sqlSessionFactory.getConfiguration().getMapperRegistry().getMappers();
			for (Class<?> mapper : mappers) {
				container.registerSingleton(Mapper.class.getSimpleName(), mapper.getName(), getMapper(mapper));
			}
		}
	}

	/**
	 * 用于从连接池中获取一个会自动提交更改的数据库Session。
	 * 
	 * @return 会自动提交的数据库Session
	 */
	private SqlSession getSession() {
		if (sqlSessionFactory == null) {
			FrameLogger.error("DatabaseManager未正确配置！");
			throw new RuntimeException("DatabaseManager未正确配置！");
		}
		return sqlSessionFactory.openSession(true);
	}

	/**
	 * 用于从连接池中获取一个非自动提交的数据库Session，相当于开启一个事务。
	 * 
	 * @return 非自动提交的数据库Session
	 */
	private SqlSession getTranSession() {
		if (sqlSessionFactory == null) {
			FrameLogger.error("DatabaseManager未正确配置！");
			throw new RuntimeException("DatabaseManager未正确配置！");
		}
		return sqlSessionFactory.openSession(false);
	}

	/**
	 * 用于打开一个共享Session，该Session将被之后该线程的所有非事务操作所共享。<br/>
	 * 该操作一般用于降低Session重复创建的次数，但在使用后需要适时的调用{@link #closeSharedSession()}关闭Session。
	 */
	@Override
	public void openSharedSession() {
		sharedSession.set(getSession());
	}

	/**
	 * 用于关闭一个被{@link #openSharedSession()} 所打开的Session，之后该线程的所有非事务操作将自行新建Session独立运行。
	 */
	@Override
	public void closeSharedSession() {
		SqlSession session = sharedSession.get();
		if (session != null) {
			session.close();
		}
		sharedSession.remove();
	}

	/**
	 * 用于获取一个会自动提交更改的Mapper，该Mapper线程安全且可以长期持有。
	 * 
	 * @return 会自动提交更改的Mapper
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> T getMapper(Class<T> clazz) {
		return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[] { clazz }, new MapperProxy(clazz));
	}

	/**
	 * 初始化一个事务对象。<br/>
	 * 注意：该事务会在提交或回滚后自动释放连接。
	 */
	@Override
	public Transaction getTransaction() {
		return new DefaultTransaction(true);
	}

	/**
	 * 根据参数，初始化一个事务对象。（除非您了解该功能，否则不推荐使用非自动关闭的事务）
	 * 
	 * @param autoClose
	 *           是否需要在提交或回滚后自动释放数据库连接。
	 */
	@Override
	public Transaction getTransaction(boolean autoClose) {
		return new DefaultTransaction(autoClose);
	}

	/**
	 * 用于从数据库查询一条数据。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @return 返回一个sqlmap中定义的类型的结果 当没有数据返回时，函数返回null。<br/>
	 *         值得注意的是，当数据多于一条时，会抛出异常： {@link org.apache.ibatis.exceptions.TooManyResultsException}
	 */
	@Override
	public <T> T selectOne(String sqlmap) {
		return selectOne(sqlmap, null);
	}

	/**
	 * 用于从数据库查询一条数据。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @param parameter
	 *           需要传入的参数对象，与sqlmap中定义的类型相同
	 * @return 返回一个sqlmap中定义的类型的结果 当没有数据返回时，函数返回null。<br/>
	 *         值得注意的是，当数据多于一条时，会抛出异常： {@link org.apache.ibatis.exceptions.TooManyResultsException}
	 */
	@Override
	public <T> T selectOne(String sqlmap, Object parameter) {
		SqlSession tempSession = sharedSession.get();
		T result;
		if (tempSession == null) {
			tempSession = getSession();
			try {
				result = tempSession.selectOne(sqlmap, parameter);
			} finally {
				tempSession.close();
			}
		} else {
			result = tempSession.selectOne(sqlmap, parameter);
		}
		return result;
	}

	/**
	 * 用于从数据库查询一些数据。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @return 返回一个sqlmap中定义的类型的结果的List类型对象。 当没有数据返回时，函数返回一个空的List。
	 */
	@Override
	public <E> List<E> selectList(String sqlmap) {
		return selectList(sqlmap, null);
	}

	/**
	 * 用于从数据库查询一些数据。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @param parameter
	 *           需要传入的参数对象，与sqlmap中定义的类型相同
	 * @return 返回一个sqlmap中定义的类型的结果的List类型对象。 当没有数据返回时，函数返回一个空的List。
	 */
	@Override
	public <E> List<E> selectList(String sqlmap, Object parameter) {
		SqlSession tempSession = sharedSession.get();
		List<E> result;
		if (tempSession == null) {
			tempSession = getSession();
			try {
				result = tempSession.selectList(sqlmap, parameter);
			} finally {
				tempSession.close();
			}
		} else {
			result = tempSession.selectList(sqlmap, parameter);
		}
		return result;
	}

	/**
	 * 用于插入数据，执行失败将抛出运行时异常。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @return 返回被影响的行数。
	 */
	@Override
	public int insert(String sqlmap) {
		return insert(sqlmap, null);
	}

	/**
	 * 用于插入数据，执行失败将抛出运行时异常。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @param parameter
	 *           需要传入的参数对象，与sqlmap中定义的类型相同
	 * @return 返回被影响的行数。
	 */
	@Override
	public int insert(String sqlmap, Object parameter) {
		SqlSession tempSession = sharedSession.get();
		int result;
		if (tempSession == null) {
			tempSession = getSession();
			try {
				result = tempSession.insert(sqlmap, parameter);
			} finally {
				tempSession.close();
			}
		} else {
			result = tempSession.insert(sqlmap, parameter);
		}
		return result;
	}

	/**
	 * 用于更新数据，执行失败将抛出运行时异常。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @return 返回被影响的行数。
	 */
	@Override
	public int update(String sqlmap) {
		return update(sqlmap, null);
	}

	/**
	 * 用于更新数据，执行失败将抛出运行时异常。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @param parameter
	 *           需要传入的参数对象，与sqlmap中定义的类型相同
	 * @return 返回被影响的行数。
	 */
	@Override
	public int update(String sqlmap, Object parameter) {
		SqlSession tempSession = sharedSession.get();
		int result;
		if (tempSession == null) {
			tempSession = getSession();
			try {
				result = tempSession.update(sqlmap, parameter);
			} finally {
				tempSession.close();
			}
		} else {
			result = tempSession.update(sqlmap, parameter);
		}
		return result;
	}

	/**
	 * 用于删除数据，执行失败将抛出运行时异常。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @return 返回被影响的行数。
	 */
	@Override
	public int delete(String sqlmap) {
		return delete(sqlmap, null);
	}

	/**
	 * 用于删除数据，执行失败将抛出运行时异常。
	 * 
	 * @param sqlmap
	 *           需要调用的sqlmap名称
	 * @param parameter
	 *           需要传入的参数对象，与sqlmap中定义的类型相同
	 * @return 返回被影响的行数。
	 */
	@Override
	public int delete(String sqlmap, Object parameter) {
		SqlSession tempSession = sharedSession.get();
		int result;
		if (tempSession == null) {
			tempSession = getSession();
			try {
				result = tempSession.delete(sqlmap, parameter);
			} finally {
				tempSession.close();
			}
		} else {
			result = tempSession.delete(sqlmap, parameter);
		}
		return result;
	}

	/**
	 * 实现了具名事务处理。没有实现线程安全。
	 * 
	 * @author carl.tian
	 * 
	 */
	public class DefaultTransaction implements Transaction {
		private SqlSession session;
		private final boolean autoClose;
		private final HashMap<CallbackType, List<Callback>> callbackMap = new HashMap<CallbackType, List<Callback>>();

		/**
		 * 根据参数，初始化一个事务对象。（除非您了解该功能，否则不推荐使用非自动关闭的事务）
		 * 
		 * @param autoClose
		 *           是否需要在提交或回滚后自动释放数据库连接。
		 */
		private DefaultTransaction(boolean autoClose) {
			this.autoClose = autoClose;
			// 初始化callback列表
			for (CallbackType type : CallbackType.values()) {
				callbackMap.put(type, new ArrayList<Callback>());
			}
		}

		/**
		 * 用于提交事务，自开启事务或上次关闭连接之后执行的更改，将被提交到数据库。
		 */
		public void commit() {
			if (session == null) {
				FrameLogger.warn("提交了一个空事务！");
			} else {
				invokeCallback(CallbackType.beforeCommit);
				session.commit();
				invokeCallback(CallbackType.afterCommit);
				// 废弃全部回调函数
				clearCallback(null);
				if (autoClose) {
					close();
				}
			}
		}

		/**
		 * 用于回滚事务，自开启事务或上次关闭连接之后执行的更改，将被丢弃。
		 */
		public void rollback() {
			if (session == null) {
				FrameLogger.warn("回滚了一个空事务！");
			} else {
				invokeCallback(CallbackType.beforeRollback);
				session.rollback();
				invokeCallback(CallbackType.afterRollback);
				// 废弃全部回调函数
				clearCallback(null);
				if (autoClose) {
					close();
				}
			}
		}

		/**
		 * 用于关闭事务所占用的数据库连接。<br/>
		 * 如果设置自动关闭为假（不推荐），则需要在事务废弃前，手动调用该函数关闭事务。
		 */
		public void close() {
			if (session == null) {
				FrameLogger.warn("重复关闭了事务！");
			} else {
				// 如果有callback则回调，证明事务没有提交过，直接关闭
				invokeCallback(CallbackType.beforeRollback);
				session.close();
				invokeCallback(CallbackType.afterRollback);
				session = null;
				// 废弃全部回调函数
				clearCallback(null);
			}
		}

		/**
		 * 用于获取属于当前事务的Mapper
		 * 
		 * @return 属于当前事务的Mapper
		 */
		public <T> T getMapper(Class<T> clazz) {
			if (session == null) {
				session = getTranSession();
			}
			return session.getMapper(clazz);
		}

		/**
		 * 用于从数据库查询一条数据。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @return 返回一个sqlmap中定义的类型的结果 当没有数据返回时，函数返回null。<br/>
		 *         值得注意的是，当数据多于一条时，会抛出异常： {@link org.apache.ibatis.exceptions.TooManyResultsException}
		 */
		public <T> T selectOne(String sqlmap) {
			return selectOne(sqlmap, null);
		}

		/**
		 * 用于从数据库查询一条数据。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @param parameter
		 *           需要传入的参数对象，与sqlmap中定义的类型相同
		 * @return 返回一个sqlmap中定义的类型的结果 当没有数据返回时，函数返回null。<br/>
		 *         值得注意的是，当数据多于一条时，会抛出异常： {@link org.apache.ibatis.exceptions.TooManyResultsException}
		 */
		public <T> T selectOne(String sqlmap, Object parameter) {
			if (session != null) {
				return session.selectOne(sqlmap, parameter);
			} else {
				SqlSession tempSession = sharedSession.get();
				T result;
				if (tempSession == null) {
					tempSession = getSession();
					try {
						result = tempSession.selectOne(sqlmap, parameter);
					} finally {
						tempSession.close();
					}
				} else {
					result = tempSession.selectOne(sqlmap, parameter);
				}
				return result;
			}
		}

		/**
		 * 用于从数据库查询一些数据。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @return 返回一个sqlmap中定义的类型的结果的List类型对象。 当没有数据返回时，函数返回一个空的List。
		 */
		public <E> List<E> selectList(String sqlmap) {
			return selectList(sqlmap, null);
		}

		/**
		 * 用于从数据库查询一些数据。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @param parameter
		 *           需要传入的参数对象，与sqlmap中定义的类型相同
		 * @return 返回一个sqlmap中定义的类型的结果的List类型对象。 当没有数据返回时，函数返回一个空的List。
		 */
		public <E> List<E> selectList(String sqlmap, Object parameter) {
			if (session != null) {
				return session.selectList(sqlmap, parameter);
			} else {
				SqlSession tempSession = sharedSession.get();
				List<E> result;
				if (tempSession == null) {
					tempSession = getSession();
					try {
						result = tempSession.selectList(sqlmap, parameter);
					} finally {
						tempSession.close();
					}
				} else {
					result = tempSession.selectList(sqlmap, parameter);
				}
				return result;
			}
		}

		/**
		 * 用于插入数据，执行失败将抛出运行时异常。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @return 返回被影响的行数。
		 */
		public int insert(String sqlmap) {
			return insert(sqlmap, null);
		}

		/**
		 * 用于插入数据，执行失败将抛出运行时异常。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @param parameter
		 *           需要传入的参数对象，与sqlmap中定义的类型相同
		 * @return 返回被影响的行数。
		 */
		public int insert(String sqlmap, Object parameter) {
			int result;
			if (session == null) {
				session = getTranSession();
			}
			result = session.insert(sqlmap, parameter);
			return result;
		}

		/**
		 * 用于更新数据，执行失败将抛出运行时异常。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @return 返回被影响的行数。
		 */
		public int update(String sqlmap) {
			return update(sqlmap, null);
		}

		/**
		 * 用于更新数据，执行失败将抛出运行时异常。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @param parameter
		 *           需要传入的参数对象，与sqlmap中定义的类型相同
		 * @return 返回被影响的行数。
		 */
		public int update(String sqlmap, Object parameter) {
			int result;
			if (session == null) {
				session = getTranSession();
			}
			result = session.update(sqlmap, parameter);
			return result;
		}

		/**
		 * 用于删除数据，执行失败将抛出运行时异常。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @return 返回被影响的行数。
		 */
		public int delete(String sqlmap) {
			return delete(sqlmap, null);
		}

		/**
		 * 用于删除数据，执行失败将抛出运行时异常。
		 * 
		 * @param sqlmap
		 *           需要调用的sqlmap名称
		 * @param parameter
		 *           需要传入的参数对象，与sqlmap中定义的类型相同
		 * @return 返回被影响的行数。
		 */
		public int delete(String sqlmap, Object parameter) {
			int result;
			if (session == null) {
				session = getTranSession();
			}
			result = session.delete(sqlmap, parameter);
			return result;
		}

		/**
		 * 用于增加事务的各种回调函数（非静态）。<br/>
		 * 回调函数只对当前批次的数据有效，当数据被提交或回滚后，全部回调函数（无论是否被执行过）都将失效。<br/>
		 * 可以绑定多个回调函数，将按照绑定的先后顺序调用。<br/>
		 * 值得注意的是，回调函数必须为公有的无参函数。
		 * 
		 * @param callbackObj
		 *           回调函数所在的对象（非静态函数回调）
		 * @param methodName
		 *           回调函数名称
		 * @param type
		 *           回调类型
		 */
		public void addCallback(Object callbackObj, String methodName, CallbackType type) {
			List<Callback> callbackList = callbackMap.get(type);
			try {
				Callback callback = new Callback(callbackObj, methodName);
				callbackList.add(callback);
			} catch (SecurityException e) {
				FrameLogger.error("无法访问您所提供的对象的函数！", e);
			} catch (NoSuchMethodException e) {
				FrameLogger.error("没有找到名称为 " + methodName + " 的无参公有函数！", e);
			}
		}

		/**
		 * 用于增加事务的各种回调函数（静态）。<br/>
		 * 回调函数只对当前批次的数据有效，当数据被提交或回滚后，全部回调函数（无论是否被执行过）都将失效。<br/>
		 * 可以绑定多个回调函数，将按照绑定的先后顺序调用。<br/>
		 * 值得注意的是，回调函数必须为公有的无参函数。
		 * 
		 * @param callbackObj
		 *           回调函数所在的类（静态函数回调）
		 * @param methodName
		 *           回调函数名称
		 * @param type
		 *           回调类型
		 */
		public void addCallback(Class<?> callbackClass, String methodName, CallbackType type) {
			List<Callback> callbackList = callbackMap.get(type);
			try {
				Callback callback = new Callback(callbackClass, methodName);
				callbackList.add(callback);
			} catch (SecurityException e) {
				FrameLogger.error("无法访问您所提供的类的函数！", e);
			} catch (NoSuchMethodException e) {
				FrameLogger.error("没有找到名称为 " + methodName + " 的无参公有函数！", e);
			}
		}

		private void invokeCallback(CallbackType type) {
			List<Callback> callbackList = callbackMap.get(type);
			for (Callback callback : callbackList) {
				try {
					callback.invoke();
				} catch (Exception e) {
					FrameLogger.error("回调函数调用失败！可能您调用了非静态方法，但未提供对象实例。", e);
				}
			}
			clearCallback(type);
		}

		private void clearCallback(CallbackType type) {
			if (type != null) {
				callbackMap.get(type).clear();
			} else {
				for (List<Callback> callbackList : callbackMap.values()) {
					callbackList.clear();
				}
			}
		}

		private class Callback {
			private final Object callbackObj;
			private final Method method;

			public Callback(Object callbackObj, String methodName) throws SecurityException, NoSuchMethodException {
				this.callbackObj = callbackObj;
				method = callbackObj.getClass().getMethod(methodName);
			}

			public Callback(Class<?> callbackClass, String methodName) throws SecurityException, NoSuchMethodException {
				callbackObj = null;
				method = callbackClass.getMethod(methodName);
			}

			public void invoke() throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
				method.invoke(callbackObj);
			}
		}

		@Override
		protected void finalize() throws Throwable {
			if (session != null) {
				FrameLogger.error(this.toString() + ":检查到没有关闭连接的具名事务对象被销毁！请检查代码。");
				session.close();
				session = null;
			}
			super.finalize();
		}
	}

	/**
	 * 代理Mapper操作，实现每个动作都打开一个Session去完成。
	 * 
	 * @author carl.tian
	 * 
	 */
	private class MapperProxy implements InvocationHandler {

		private final Class<?> mapperClass;

		private MapperProxy(Class<?> mapperClass) {
			this.mapperClass = mapperClass;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			SqlSession tempSession = sharedSession.get();
			Object result;
			if (tempSession == null) {
				tempSession = getSession();
				try {
					result = method.invoke(tempSession.getMapper(mapperClass), args);
				} finally {
					tempSession.close();
				}
			} else {
				result = method.invoke(tempSession.getMapper(mapperClass), args);
			}
			return result;
		}
	}
}
