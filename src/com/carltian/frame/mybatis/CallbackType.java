package com.carltian.frame.mybatis;

/**
 * 枚举类型，用于区分不同类型的回调函数
 */
public enum CallbackType {
	afterCommit, beforeCommit, afterRollback, beforeRollback
}