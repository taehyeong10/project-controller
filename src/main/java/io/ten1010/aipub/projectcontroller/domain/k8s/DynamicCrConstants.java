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
   * 선정 기준: (1) 멤버 Role 이 create 를 허용하고 (2) 사용자가 직접 관리하는 오브젝트이며
   * (3) 낙인 웹훅이 CREATE 를 인터셉트하는 타입. 이 기준에 따라
   * 시스템 파생물(events/endpoints/endpointslices/leases/controllerrevisions)과
   * 워크로드 파생물(pods/replicasets — 개인 Role 비대 방지, 삭제는 부모 삭제로 캐스케이드),
   * 멤버가 생성 불가한 타입(networkpolicies/resourcequotas/roles 등)은 제외한다.
   * jobs/cronjobs 는 기존 정적 경로가 담당한다.
   * 목록 추가 시 낙인 웹훅 rules(UserLabelWebhookConfigurationReconciler.baseRules,
   * mutating-webhook-user-v2.yaml)가 해당 타입을 커버하는지 함께 확인할 것.
   */
  public static final List<ResourceTarget> NATIVE_OWNED_TARGETS = List.of(
      new ResourceTarget("", "v1", "configmaps", true),
      new ResourceTarget("", "v1", "secrets", true),
      new ResourceTarget("", "v1", "services", true),
      new ResourceTarget("", "v1", "persistentvolumeclaims", true),
      new ResourceTarget("", "v1", "replicationcontrollers", true),
      new ResourceTarget("", "v1", "serviceaccounts", true),
      new ResourceTarget("", "v1", "limitranges", true),
      new ResourceTarget("apps", "v1", "deployments", true),
      new ResourceTarget("apps", "v1", "statefulsets", true),
      new ResourceTarget("apps", "v1", "daemonsets", true),
      new ResourceTarget("networking.k8s.io", "v1", "ingresses", true),
      new ResourceTarget("autoscaling", "v2", "horizontalpodautoscalers", true),
      new ResourceTarget("policy", "v1", "poddisruptionbudgets", true));

  private DynamicCrConstants() {
  }

}
