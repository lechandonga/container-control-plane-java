package com.github.lechandonga.containercontrolplanejava.web.dto;

public record ErrorView(String error, String reason, String workloadUid,
                        String suppliedResourceVersion, String currentResourceVersion) {}
