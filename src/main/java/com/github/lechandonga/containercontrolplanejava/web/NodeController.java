package com.github.lechandonga.containercontrolplanejava.web;

import com.github.lechandonga.containercontrolplanejava.api.ControlPlaneService;
import com.github.lechandonga.containercontrolplanejava.node.NodeRegistry;
import com.github.lechandonga.containercontrolplanejava.node.SimulatedNode;
import com.github.lechandonga.containercontrolplanejava.web.dto.NodeRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/nodes")
public class NodeController {

    private final ControlPlaneService service;
    private final NodeRegistry nodeRegistry;

    public NodeController(ControlPlaneService service, NodeRegistry nodeRegistry) {
        this.service = service;
        this.nodeRegistry = nodeRegistry;
    }

    @PostMapping
    public Map<String, Object> register(@RequestBody NodeRequest request) {
        if (request.capacity() == null || request.capacity() <= 0) {
            throw new IllegalArgumentException("capacity 必须为正整数");
        }
        String host = request.host() != null ? request.host() : "simulated-host";
        SimulatedNode node = service.registerNode(host, request.capacity());
        return Map.of("uid", node.getInfo().getUid(), "host", host,
                "capacity", request.capacity(), "state", node.getInfo().getState().name());
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return nodeRegistry.all().stream()
                .map(n -> Map.<String, Object>of(
                        "uid", n.getInfo().getUid(),
                        "host", n.getInfo().getHost(),
                        "capacity", n.getInfo().getCapacity(),
                        "used", n.runningCount(),
                        "state", n.getInfo().getState().name(),
                        "partitioned", n.isPartitioned()))
                .toList();
    }

    /** 故障注入：节点网络分区（失联）。 */
    @PostMapping("/{uid}/partition")
    public Map<String, String> partition(@PathVariable String uid) {
        service.partitionNode(uid);
        return Map.of("uid", uid, "result", "PARTITIONED");
    }

    /** 节点网络恢复。 */
    @PostMapping("/{uid}/recover")
    public Map<String, String> recover(@PathVariable String uid) {
        service.recoverNode(uid);
        return Map.of("uid", uid, "result", "RECOVERED");
    }

    /** 故障注入：指定镜像在该节点启动失败。 */
    @PostMapping("/{uid}/fail-image")
    public Map<String, String> failImage(@PathVariable String uid,
                                         @RequestParam String image) {
        service.failImageOnNode(uid, image);
        return Map.of("uid", uid, "image", image, "result", "IMAGE_WILL_FAIL");
    }
}
