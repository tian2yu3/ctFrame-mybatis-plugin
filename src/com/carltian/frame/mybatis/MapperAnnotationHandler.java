package com.carltian.frame.mybatis;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;

import com.carltian.frame.container.InjectAnnotationHandler;
import com.carltian.frame.container.reg.ArgInfo;

public class MapperAnnotationHandler extends InjectAnnotationHandler {

	@Override
	public ArgInfo parseField(ArgInfo argInfo, Annotation annotation, Field field) {
		return parse(field.getGenericType(), argInfo);
	}

	@Override
	public ArgInfo parseParam(ArgInfo argInfo, Annotation annotation, Type paramType, Annotation[] paramAnnotations) {
		return parse(paramType, argInfo);
	}

	private ArgInfo parse(Type type, ArgInfo argInfo) {
		// 设置默认值
		if (argInfo.getValue() == Object.class) {
			if (type instanceof Class<?>) {
				argInfo.setValue(type);
			} else {
				throw new UnsupportedOperationException("抱歉，暂时不支持依据参数化类型等特殊形式的类型确定Mapper名称，请明确指出需要注入的Mapper类型，或使用普通类型定义参数");
			}
		}
		// 转换类型
		if (argInfo != null) {
			argInfo.setValue(((Class<?>) argInfo.getValue()).getName());
		}
		return argInfo;
	}

}
