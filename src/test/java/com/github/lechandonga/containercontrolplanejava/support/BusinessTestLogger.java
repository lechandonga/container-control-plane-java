package com.github.lechandonga.containercontrolplanejava.tests.support;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * 让单测日志能够直观看出：
 * <ol>
 *   <li>每个测试类覆盖的是哪块<b>业务域</b>（@BusinessUnderTest）；</li>
 *   <li>每个用例的场景名称、执行结果；</li>
 *   <li>测试类结束时的用例汇总（通过/失败数）。</li>
 * </ol>
 * 输出使用统一前缀 [BUSINESS-TESTS]，便于从 Maven 日志中检索覆盖情况。
 */
public class BusinessTestLogger implements BeforeAllCallback, BeforeEachCallback,
        AfterEachCallback, AfterAllCallback {

    private static final String PREFIX = "[BUSINESS-TESTS] ";

    private final List<String> passed = new ArrayList<>();
    private final List<String> failed = new ArrayList<>();

    @Override
    public void beforeAll(ExtensionContext context) {
        String business = context.getTestClass()
                .map(c -> c.isAnnotationPresent(BusinessUnderTest.class)
                        ? c.getAnnotation(BusinessUnderTest.class).value()
                        : "未标注业务域").orElse("未标注业务域");
        System.out.println();
        System.out.println(PREFIX + "========== 业务域测试开始 ==========");
        System.out.println(PREFIX + "测试类  : " + context.getDisplayName());
        System.out.println(PREFIX + "业务覆盖: " + business);
        System.out.println(PREFIX + "--------------------------------------------------");
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        String scenario = context.getTestMethod()
                .map(m -> m.isAnnotationPresent(Scenario.class)
                        ? m.getAnnotation(Scenario.class).value()
                        : m.getName()).orElse(context.getDisplayName());
        System.out.println(PREFIX + "▶ 用例: " + scenario);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        String scenario = context.getTestMethod()
                .map(m -> m.isAnnotationPresent(Scenario.class)
                        ? m.getAnnotation(Scenario.class).value()
                        : m.getName()).orElse(context.getDisplayName());
        boolean success = context.getExecutionException().isEmpty();
        if (success) {
            passed.add(scenario);
            System.out.println(PREFIX + "  ✔ 结果: 通过");
        } else {
            failed.add(scenario);
            Throwable error = context.getExecutionException().get();
            System.out.println(PREFIX + "  ✗ 结果: 失败 -> " + error);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        System.out.println(PREFIX + "--------------------------------------------------");
        System.out.println(PREFIX + "用例汇总: 共 " + (passed.size() + failed.size())
                + " 个，通过 " + passed.size() + " 个，失败 " + failed.size() + " 个");
        passed.forEach(s -> System.out.println(PREFIX + "  [PASS] " + s));
        failed.forEach(s -> System.out.println(PREFIX + "  [FAIL] " + s));
        System.out.println(PREFIX + "========== 业务域测试结束 ==========");
        System.out.println();
    }
}
