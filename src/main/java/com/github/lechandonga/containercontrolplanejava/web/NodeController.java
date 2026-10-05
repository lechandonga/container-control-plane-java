package com.github.lechandonga.containercontrolplanejava.web;

import com.github.lechandonga.containercontrolplanejava.core.ControlPlane;
import com.github.lechandonga.containercontrolplanejava.model.Node;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 模拟节点管理：注册/移除节点、注入失联与恢复。
 */
@RestController
@RequestMapping("/api/nodes")
public class NodeController {
    private final ControlPlane controlPlane;

    public NodeController(ControlPlane controlPlane) {
        this.controlPlane = controlPlane;
    }

    public record RegisterNodeRequest(String id, Integer capacity, Integer startupTicks) {
    }

    @PostMapping
    public Node register(@RequestBody RegisterNodeRequest req) {
        return controlPlane.registerNode(req.id(),
                req.capacity() == null ? 1 : req.capacity(), req.startupTicks());
    }

    @PostMapping("/{id}/lost")
    public Node lost(@PathVariable String id) {
        return controlPlane.markNodeLost(id);
    }

    @PostMapping("/{id}/ready")
    public Node ready(@PathVariable String id) {
        return controlPlane.markNodeReady(id);
    }

    @DeleteMapping("/{id}")
    public Map<String, Object> remove(@PathVariable String id) {
        controlPlane.removeNode(id);
        return Map.of("removed", id);
    }
}
