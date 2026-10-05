package com.github.lechandonga.containercontrolplanejava.tests;

import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadPhase;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.store.ConflictException;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessTestLogger;
import com.github.lechandonga.containercontrolplanejava.tests.support.BusinessUnderTest;
import com.github.lechandonga.containercontrolplanejava.tests.support.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务域：并发修改下的一致性。乐观锁拒绝过期版本，冲突明确可区分，绝不静默覆盖。
 */
@BusinessUnderTest("并发一致性：扩缩容/改版本/删除并发时乐观锁拒绝冲突更新，无丢失更新、无脑裂")
@ExtendWith(BusinessTestLogger.class)
class ConcurrentConflictTest extends ControlPlaneTestBase {

    @Test
    @Scenario("基于过期 resourceVersion 的更新被明确拒绝（409 语义），原状态不被覆盖")
    void staleResourceVersionIsRejected() {
        node(4);
        service.createWorkload("wl-cc-1", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-cc-1", 20);

        String stale = store.get("wl-cc-1").getResourceVersion();
        // 第一个更新成功，推进了 resourceVersion
        service.submit("wl-cc-1", WorkloadSpec.of(3, "app:v1"), stale);
        tickUntilConverged("wl-cc-1", 20);

        // 第二个更新仍带着旧版本，必须被拒绝
        ConflictException ex = assertThrows(ConflictException.class,
                () -> service.submit("wl-cc-1", WorkloadSpec.of(2, "app:v9"), stale));
        assertTrue(ex.getMessage().contains("并发修改冲突"));
        assertEquals(stale, ex.getSuppliedResourceVersion());
        assertFalse(ex.getCurrentResourceVersion().equals(stale));

        // 被拒绝的 v9/2 副本不得生效
        assertEquals("app:v1", store.get("wl-cc-1").getSpec().image());
        assertEquals(3, store.get("wl-cc-1").getSpec().replicas());
    }

    @Test
    @Scenario("多线程并发扩缩容：恰好一个提交成功，其余全部收到冲突异常，最终状态确定无丢失")
    void concurrentScalingExactlyOneWins() throws Exception {

        node(12);
        service.createWorkload("wl-cc-2", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-cc-2", 20);
        String rv = store.get("wl-cc-2").getResourceVersion();

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        ConcurrentLinkedQueue<Integer> acceptedReplicas = new ConcurrentLinkedQueue<>();
        AtomicInteger rejected = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final int target = i + 3; // 3..10
            pool.submit(() -> {
                try {
                    start.await();
                    Workload wl = service.submit("wl-cc-2",
                            WorkloadSpec.of(target, "app:v1"), rv);
                    accepted.incrementAndGet();
                    acceptedReplicas.add(wl.getSpec().replicas());
                } catch (ConflictException e) {
                    rejected.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, accepted.get(), "同一基版本的并发提交只能有一个胜出");
        assertEquals(threads - 1, rejected.get(), "其余全部必须收到明确冲突拒绝");
        tickUntilConverged("wl-cc-2", 40);
        int winner = acceptedReplicas.peek();
        assertEquals(winner, store.get("wl-cc-2").getSpec().replicas(),
                "胜出的副本数必须是最终状态，不存在丢失更新");
        assertEquals(winner, countAvailable("wl-cc-2"));
    }

    @Test
    @Scenario("改版本与删除并发：删除后更新得到 NOT_FOUND，已删除工作负载不得被“复活”")
    void updateAfterDeleteDoesNotResurrect() {
        node(3);
        service.createWorkload("wl-cc-3", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-cc-3", 20);
        String rv = store.get("wl-cc-3").getResourceVersion();

        service.delete("wl-cc-3", rv);
        tick(20);

        assertTrue(store.find("wl-cc-3").isEmpty(), "删除完成后聚合必须物理消失");
        assertThrows(com.github.lechandonga.containercontrolplanejava.store.NotFoundException.class,
                () -> service.submit("wl-cc-3", WorkloadSpec.of(2, "app:v2"), rv),
                "删除后的并发更新必须被明确判定为资源不存在，不得静默重建");
    }

    @Test
    @Scenario("冲突重试规则：调用方读取最新 resourceVersion 后重试，更新被接受并正常收敛")
    void conflictCanBeResolvedByReadModifyWrite() {
        node(4);
        service.createWorkload("wl-cc-4", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-cc-4", 20);

        String rv = store.get("wl-cc-4").getResourceVersion();
        service.submit("wl-cc-4", WorkloadSpec.of(2, "app:v2"), rv); // 推进版本
        Workload latest = store.get("wl-cc-4");

        assertThrows(ConflictException.class,
                () -> service.submit("wl-cc-4", WorkloadSpec.of(3, "app:v3"), rv));

        // 读-改-写：基于最新版本重试
        service.submit("wl-cc-4", WorkloadSpec.of(3, "app:v3"), latest.getResourceVersion());
        tickUntilConverged("wl-cc-4", 40);

        assertEquals(3, store.get("wl-cc-4").getSpec().replicas());
        assertEquals("app:v3", store.get("wl-cc-4").getSpec().image());
        assertEquals(3, countImage("wl-cc-4", "app:v3"));
        assertEquals(WorkloadPhase.RUNNING, store.get("wl-cc-4").getPhase());
    }

    @Test
    @Scenario("无版本号的盲写更新必须被拒绝，不能作为覆盖入口")
    void blindWriteWithoutVersionRejected() {
        node(2);
        service.createWorkload("wl-cc-5", WorkloadSpec.of(2, "app:v1"));
        tickUntilConverged("wl-cc-5", 20);
        assertThrows(ConflictException.class,
                () -> service.submit("wl-cc-5", WorkloadSpec.of(2, "app:v2"), null));
    }
}
