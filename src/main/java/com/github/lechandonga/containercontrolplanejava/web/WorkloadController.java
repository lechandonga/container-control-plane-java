package com.github.lechandonga.containercontrolplanejava.web;

import com.github.lechandonga.containercontrolplanejava.core.ControlPlane;
import com.github.lechandonga.containercontrolplanejava.core.WorkloadRequest;
import com.github.lechandonga.containercontrolplanejava.exception.ApiException;
import com.github.lechandonga.containercontrolplanejava.model.Event;
import com.github.lechandonga.containercontrolplanejava.model.Node;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 工作负载期望状态 API 与只读观测 API。
 */
@RestController
@RequestMapping("/api")
public class WorkloadController {
    private static final long DEFAULT_START_TIMEOUT = 5;
    private static final long DEFAULT_ROLLOUT_TIMEOUT = 20;

    private final ControlPlane controlPlane;

    public WorkloadController(ControlPlane controlPlane) {
        this.controlPlane = controlPlane;
    }

    public record SubmitRequest(
            String id,
            String image,
            String version,
            Integer replicas,
            Integer minAvailable,
            Long startTimeoutTicks,
            Long rolloutTimeoutTicks,
            Long resourceVersion
    ) {
    }

    /** 创建与更新是同一个幂等端点（PUT 语义）。 */
    @PutMapping("/workloads/{id}")
    public Workload submit(@PathVariable String id, @RequestBody SubmitRequest req) {
        int replicas = req.replicas() == null ? 1 : req.replicas();
        int minAvailable = req.minAvailable() == null ? 1 : req.minAvailable();
        WorkloadRequest wr = new WorkloadRequest(
                id,
                req.image(),
                req.version(),
                replicas,
                minAvailable,
                req.startTimeoutTicks() == null ? DEFAULT_START_TIMEOUT : req.startTimeoutTicks(),
                req.rolloutTimeoutTicks() == null ? DEFAULT_ROLLOUT_TIMEOUT : req.rolloutTimeoutTicks(),
                req.resourceVersion()
        );
        return controlPlane.submit(wr);
    }

    @DeleteMapping("/workloads/{id}")
    public Map<String, Object> delete(@PathVariable String id) {
        controlPlane.delete(id);
        return Map.of("deleted", id);
    }

    @GetMapping("/workloads")
    public Map<String, Workload> list() {
        return controlPlane.snapshotView().getWorkloads();
    }

    @GetMapping("/workloads/{id}")
    public Workload get(@PathVariable String id) {
        Workload w = controlPlane.snapshotView().getWorkloads().get(id);
        if (w == null) {
            throw ApiException.notFound("工作负载不存在: " + id);
        }
        return w;
    }

    @GetMapping("/nodes")
    public Map<String, Node> nodes() {
        return controlPlane.snapshotView().getNodes();
    }

    @GetMapping("/events")
    public List<Event> events(@RequestParam(required = false) String workloadId) {
        List<Event> events = controlPlane.snapshotView().getEvents();
        if (workloadId == null) {
            return List.copyOf(events);
        }
        return events.stream().filter(e -> workloadId.equals(e.workloadId())).toList();
    }
}
