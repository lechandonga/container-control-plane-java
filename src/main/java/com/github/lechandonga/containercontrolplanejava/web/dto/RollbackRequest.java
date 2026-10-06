package com.github.lechandonga.containercontrolplanejava.web.dto;

/**
 * 按修订回滚请求体。
 *
 * @param revision        目标修订号；为 null 表示“退回上一个修订”
 * @param resourceVersion 乐观锁版本（必填，规则与 PUT 更新一致）
 */
public record RollbackRequest(Long revision, String resourceVersion) {}
