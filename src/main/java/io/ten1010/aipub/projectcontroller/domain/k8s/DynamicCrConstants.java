package io.ten1010.aipub.projectcontroller.domain.k8s;

import java.util.Set;

public final class DynamicCrConstants {

  /**
   * 동적 CR 소유권 처리(레이블 낙인 웹훅 rules, 개인 Role/ClusterRole 규칙 생성)에서 제외하는
   * 플랫폼 컨트롤 플레인 그룹. aipub.ten1010.io 의 워크로드 타입은 기존 정적 경로
   * (WorkloadResourceResolver + AipubUserRoleReconciler)가 담당하며, 나머지 타입(검증·리뷰류)은
   * 생성 즉시 GC 되는 일시 오브젝트라 소유권 규칙 대상이 아니다.
   */
  public static final Set<String> EXCLUDED_GROUPS = Set.of(
      ProjectApiConstants.PROJECT_GROUP,
      ProjectApiConstants.AIPUB_GROUP,
      ProjectApiConstants.COASTER_GROUP);

  private DynamicCrConstants() {
  }

}
