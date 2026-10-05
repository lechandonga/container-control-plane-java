package com.github.lechandonga.containercontrolplanejava.support;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

/**
 * 让 mvn test 的控制台日志能直接看出：
 * 覆盖了哪个业务（测试类）、业务下有哪些用例（测试方法）、每个用例通过/失败情况。
 */
public class BusinessLoggingExtension implements BeforeAllCallback, BeforeEachCallback,
        AfterEachCallback, AfterAllCallback, TestExecutionExceptionHandler {

    private final AtomicInteger passed = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();

    @Override
    public void beforeAll(ExtensionContext context) {
        System.out.println();
        System.out.println("══════════════════════════════════════════════════════════════");
        System.out.println("▶ 业务覆盖: " + context.getDisplayName());
        System.out.println("══════════════════════════════════════════════════════════════");
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        System.out.println("  ┌─ 用例: " + context.getDisplayName());
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (context.getExecutionException().isEmpty()) {
            passed.incrementAndGet();
            System.out.println("  └─ 结果: ✅ 通过");
        }
    }

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        failed.incrementAndGet();
        System.out.println("  └─ 结果: ❌ 失败 -> " + throwMessage(throwable));
        throw throwable;
    }

    @Override
    public void afterAll(ExtensionContext context) {
        int total = passed.get() + failed.get();
        System.out.println("──────────────────────────────────────────────────────────────");
        System.out.println("■ 业务[" + context.getDisplayName() + "] 用例数=" + total
                + "，通过=" + (total - failed.get()) + "，失败=" + failed.get()
                + (failed.get() == 0 ? " ✅ 全部通过" : " ❌ 存在失败"));
        System.out.println("──────────────────────────────────────────────────────────────");
        System.out.println();
    }

    private String throwMessage(Throwable t) {
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
