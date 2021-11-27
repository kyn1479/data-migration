package com.kkk.data.migration.utils;

import org.assertj.core.util.Lists;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
public class FieldMappingUtil {

    public static <T extends Annotation> T getAnnotation(Class<T> annotationClass, Field field, Class<?> beanClass) {
        PropertyDescriptor propertyDescriptor = null;

        try {
            propertyDescriptor = new PropertyDescriptor(field.getName(), beanClass);
        } catch (IntrospectionException var6) {
            var6.printStackTrace();
        }

        Method getMethod = propertyDescriptor.getReadMethod();
        T t = null;
        t = getMethod.getAnnotation(annotationClass);
        if (null == t) {
            t = field.getAnnotation(annotationClass);
        }

        return t;
    }

    public static List<Field> getFields(Class<?> c) {
        if (c == Object.class) {
            return new ArrayList();
        } else {
            List<Field> list = Lists.newArrayList(c.getDeclaredFields());
            list.addAll(getFields(c.getSuperclass()));
            return list;
        }
    }
}

