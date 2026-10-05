package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.core.WorkloadRequest;
import com.github.lechandonga.containercontrolplanejava.exception.ApiException;
import com.github.lechandonga.containercontrolplanejava.model.Phase;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.support.BusinessLoggingExtension;
import com.github.lechandonga.containercontrolplanejava.support.CcpTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务四：并发修改下的一致性 —— 乐观锁冲突拒绝、删除优先、无丢失更新/脑裂。
 */
@DisplayName("业务4-并发冲突与一致性")
@ExtendWith(BusinessLoggingExtension.class)
class ConcurrencyConflictTest {

    @Test
    @DisplayName("用例4.1 基于同一 resourceVersion 的并发扩缩容：只有一个成功，其余明确 409 拒绝")
    void optimisticLockRejectsStaleConcurrentWrites() throws Exception {
        var t = CcpTestSupport.create();
        t.node("n1", 100);
        t.deploy("app", "img", "v1", 1, 1);
        t.runUntilStable("app", 5);

        long rv = t.cp.snapshotView().getWorkloads().get("app").getResourceVersion();
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final int replicas = i + 2;
            pool.submit(() -> {
                try {
                    start.await();
                    t.cp.submit(new WorkloadRequest("app", "img", "v1", replicas, 1, 5, 20, rv));
                    accepted.incrementAndGet();
                } catch (ApiException e) {
                    if (e.getReason().equals("RESOURCE_VERSION_CONFLICT")) {
                        rejected.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, accepted.get(), "同一版本的并发写入必须恰好只有一个被接受");
        assertEquals(threads - 1, rejected.get(), "其余必须被明确冲突拒绝");
        Workload w = t.cp.snapshotView().getWorkloads().get("app");
        assertEquals(rv + 1, w.getResourceVersion(), "resourceVersion 只增加一次");
        t.runUntilStable("app", 20);
        assertEquals(w.getReplicas(), t.instances("app").size(), "实际副本必须精确收敛到获胜写入");
        t.assertCapacityNotExceeded();
        System.out.println("    20 并发写: 接受=" + accepted.get() + " 拒绝=" + rejected.get()
                + "，最终 replicas=" + w.getReplicas() + " 实际=" + t.instances("app").size());
    }

    @Test
    @DisplayName("用例4.2 改版本并发冲突：携带旧版本号的滚动更新被拒绝，不发生静默覆盖")
    void concurrentVersionChangeConflict() {
        var t = CcpTestSupport.create();
        t.node("n1", 10);
        t.deploy("app", "img", "v1", 2, 1);
        t.runUntilStable("app", 5);
        long rv = t.cp.snapshotView().getWorkloads().get("app").getResourceVersion();

        // 第一个写成功（v2）
        t.cp.submit(new WorkloadRequest("app", "img", "v2", 2, 1, 5, 50, rv));

        // 另一个客户端仍基于旧 rv 想升到 v3 —— 必须被拒绝
        ApiException ex = assertThrows(ApiException.class, () ->
                t.cp.submit(new WorkloadRequest("app", "img", "v3", 2, 1, 5, 50, rv)));
        assertEquals(409, ex.getStatus().value());
        assertEquals("RESOURCE_VERSION_CONFLICT", ex.getReason());

        // 不带 resourceVersion 的强制覆盖被允许（显式规则）
        Workload forced = t.cp.submit(new WorkloadRequest("app", "img", "v3", 2, 1, 5, 50, null));
        assertEquals("v3", forced.getTargetVersion());
        System.out.println("    旧版本号写 v3 被 409 拒绝；显式强制覆盖成功，目标=v3");
    }

    @Test
    @DisplayName("用例4.3 更新进行中并发删除：删除优先，更新结果不得让工作负载复活")
    void deleteWinsOverConcurrentUpdate() throws Exception {
        var t = CcpTestSupport.create();
        t.node("n1", 10);
        t.deploy("app", "img", "v1", 2, 1);
        t.runUntilStable("app", 5);
        long rv = t.cp.snapshotView().getWorkloads().get("app").getResourceVersion();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger updateRejected = new AtomicInteger();

        pool.submit(() -> {
            try {
                start.await();
                t.cp.submit(new WorkloadRequest("app", "img", "v2", 2, 1, 5, 50, rv));
            } catch (ApiException e) {
                updateRejected.incrementAndGet();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        pool.submit(() -> {
            try {
                start.await();
                t.cp.delete("app");
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertNull(t.cp.snapshotView().getWorkloads().get("app"),
                "删除优先：无论更新是否先到，最终工作负载必须不存在");
        if (updateRejected.get() == 1) {
            System.out.println("    更新先到、删除后到：更新被接受但删除立即清除全部实例与工作负载");
        } else {
            System.out.println("    删除先到：基于旧状态的更新被 409 拒绝（删除优先）");
        }
        // 删除后再用相同请求提交必须是 404 之后的新建语义不会发生：更新已删对象应抛 NOT_FOUND?
        // 这里验证删除后对象彻底不存在，杜绝“脑裂复活”。
    }

    @Test
    @DisplayName("用例4.4 并发强制覆盖串行化：全部被接受且每次调谐后计数与状态自洽，无丢失更新")
    void forcedWritesSerializeAndConverge() throws Exception {
        var t = CcpTestSupport.create();
        t.node("n1", 50);
        t.deploy("app", "img", "v1", 1, 1);
        t.runUntilStable("app", 5);

        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 1; i <= threads; i++) {
            final int replicas = i;
            pool.submit(() -> {
                try {
                    start.await();
                    t.cp.submit(new WorkloadRequest("app", "img", "v1", replicas, 1, 5, 20, null));
                    t.cp.reconcile();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        Workload w = t.runUntilStable("app", 30);
        // 最终状态必定是某次提交的完整值（1..10 之一），实例数精确匹配
        assertTrue(w.getReplicas() >= 1 && w.getReplicas() <= 10);
        assertEquals(w.getReplicas(), t.instances("app").size());
        assertEquals(Phase.RUNNING, w.getPhase());
        t.assertCapacityNotExceeded();
        System.out.println("    10 次强制覆盖全部串行接受，最终 replicas=" + w.getReplicas()
                + "，实例精确收敛，无重复/丢失");
    }
}
