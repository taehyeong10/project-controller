package io.ten1010.aipub.projectcontroller.domain.k8s;

/**
 * 사용자 소유(username 레이블) CR 오브젝트의 개인 Role/ClusterRole 규칙 생성용 식별자.
 */
public record OwnedCrObject(String group, String resource, String name) {
}
