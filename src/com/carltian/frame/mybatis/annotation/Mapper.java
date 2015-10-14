package com.carltian.frame.mybatis.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.carltian.frame.container.annotation.ExtTypeInject;
import com.carltian.frame.mybatis.MapperAnnotationHandler;

/**
 * Annotate a parameter or field as a database mapper.
 * 
 * @author Carl Tian
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Documented
@ExtTypeInject(handler = MapperAnnotationHandler.class)
public @interface Mapper {
	/**
	 * The class of mapper, default is the element's type.
	 */
	Class<?> value() default Object.class;
}
