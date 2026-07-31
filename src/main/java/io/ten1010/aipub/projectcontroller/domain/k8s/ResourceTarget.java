package io.ten1010.aipub.projectcontroller.domain.k8s;

/**
 * 소유권 추적 대상 리소스 타입 식별자 (CRD 유래 또는 네이티브 고정 목록).
 * group 이 빈 문자열이면 core API 그룹.
 */
public record ResourceTarget(String group, String version, String plural, boolean namespaced) {
}
