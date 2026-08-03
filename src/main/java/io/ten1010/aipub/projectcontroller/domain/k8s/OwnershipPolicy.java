package io.ten1010.aipub.projectcontroller.domain.k8s;

import java.util.List;

/**
 * 생성자 소유권(creator-ownership) RBAC 정책의 단일 정의처.
 * "어떤 타입을"(OWNED_TARGETS) "어떤 verbs 로"(OWNED_VERBS) 소유자에게 부여하는지가
 * 전부 이 파일에 모여 있다. 관측 메커니즘(인포머)은 informer.owned 패키지가 담당한다.
 */
public final class OwnershipPolicy {

  /**
   * 소유 오브젝트에 개인 Role 로 부여하는 verbs.
   * update/patch/delete 에 더해 get 을 포함하는 이유: 멤버 공통 Role 이 조회를 주지 않는
   * 타입이 목록에 들어올 수 있고, 중복 get 은 무해하기 때문이다.
   */
  public static final List<String> OWNED_VERBS = List.of("get", "update", "patch", "delete");

  /**
   * 소유권 추적 대상 네이티브 네임스페이스 리소스 16종 (고정 목록).
   * 소유자 레이블(aipub.ten1010.io/username)이 있는 오브젝트만 추적되며,
   * 레이블은 낙인 웹훅(mutating-webhook-user-v2.yaml 의 rules)이 찍는다 —
   * 목록을 바꿀 때는 웹훅 rules 가 해당 타입 CREATE 를 인터셉트하는지 함께 확인할 것.
   * events/endpoints 는 시스템 컴포넌트가 만드는 것이 대부분이라 웹훅이 인터셉트하지
   * 않으며(고빈도·비멤버 생성), 수동으로 레이블이 부여된 경우에만 소유로 취급된다.
   */
  public static final List<ResourceTarget> OWNED_TARGETS = List.of(
      new ResourceTarget("", "v1", "pods"),
      new ResourceTarget("", "v1", "configmaps"),
      new ResourceTarget("", "v1", "secrets"),
      new ResourceTarget("", "v1", "services"),
      new ResourceTarget("", "v1", "endpoints"),
      new ResourceTarget("", "v1", "serviceaccounts"),
      new ResourceTarget("", "v1", "events"),
      new ResourceTarget("", "v1", "persistentvolumeclaims"),
      new ResourceTarget("", "v1", "limitranges"),
      new ResourceTarget("apps", "v1", "deployments"),
      new ResourceTarget("apps", "v1", "replicasets"),
      new ResourceTarget("apps", "v1", "statefulsets"),
      new ResourceTarget("apps", "v1", "daemonsets"),
      new ResourceTarget("autoscaling", "v2", "horizontalpodautoscalers"),
      new ResourceTarget("policy", "v1", "poddisruptionbudgets"),
      new ResourceTarget("networking.k8s.io", "v1", "ingresses"));

  private OwnershipPolicy() {
  }

}
