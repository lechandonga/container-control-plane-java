package com.github.lechandonga.containercontrolplanejava.tests.support;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 标注测试类所覆盖的业务域，供 {@link BusinessTestLogger} 输出。 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface BusinessUnderTest {
    String value();
}
