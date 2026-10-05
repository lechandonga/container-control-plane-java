package com.github.lechandonga.containercontrolplanejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 基于本地 JSON 文件的持久化存储：写入采用“临时文件 + 原子 move”，
 * 保证调谐循环在任意时刻崩溃/重启后状态仍是上一个完整快照（无重复、无半截写）。
 *
 * <p>内存中的 {@link Snapshot} 由控制面单线程（全局锁）修改；每次状态变更后调用
 * {@link #persist(long)} 落盘。重启时 {@link #load()} 恢复全部期望状态、实例与事件。
 */
@Component
public class StateStore {
    private final Path file;
    private final ObjectMapper mapper;

    public StateStore(@Value("${ccp.store-file:target/ccp-state.json}") String path) {
        this.file = Path.of(path);
        this.mapper = new ObjectMapper();
    }

    @PostConstruct
    public void init() throws IOException {
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
    }

    public synchronized Snapshot load() {
        try {
            if (Files.exists(file)) {
                return mapper.readValue(file.toFile(), Snapshot.class);
            }
        } catch (IOException e) {
            throw new IllegalStateException("无法加载控制面状态文件: " + file, e);
        }
        return new Snapshot();
    }

    /** 原子落盘；fsync 语义由原子 move 保证崩溃后要么旧快照要么新快照。 */
    public synchronized void persist(Snapshot snapshot) {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("无法持久化控制面状态: " + file, e);
        }
    }

    public Path getFile() {
        return file;
    }
}
