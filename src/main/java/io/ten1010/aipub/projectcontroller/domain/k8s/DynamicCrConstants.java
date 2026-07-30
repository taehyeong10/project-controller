package io.ten1010.aipub.projectcontroller.domain.k8s;

import java.util.List;
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

  /**
   * CRD 와 동일한 소유권 메커니즘을 적용하는 네이티브 네임스페이스 리소스.
   * 낙인 웹훅이 이미 CREATE 를 인터셉트하는 타입만 넣는다(레이블이 없으면 추적이 무의미).
   * pods/replicasets 는 상위 워크로드가 만들어내는 파생 오브젝트라 제외(개인 Role 비대 방지,
   * 삭제는 부모 삭제로 캐스케이드). jobs/cronjobs 는 기존 정적 경로가 담당한다.
   */
  public static final List<ResourceTarget> NATIVE_OWNED_TARGETS = List.of(
      new ResourceTarget("", "v1", "configmaps", true),
      new ResourceTarget("", "v1", "secrets", true),
      new ResourceTarget("", "v1", "services", true),
      new ResourceTarget("", "v1", "persistentvolumeclaims", true),
      new ResourceTarget("", "v1", "replicationcontrollers", true),
      new ResourceTarget("apps", "v1", "deployments", true),
      new ResourceTarget("apps", "v1", "statefulsets", true),
      new ResourceTarget("apps", "v1", "daemonsets", true),
      new ResourceTarget("networking.k8s.io", "v1", "ingresses", true));

  private DynamicCrConstants() {
  }

}
