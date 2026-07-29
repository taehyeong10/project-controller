package io.ten1010.aipub.projectcontroller.controller.webhook;

import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AdmissionregistrationV1Api;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;
import io.kubernetes.client.openapi.models.V1MutatingWebhookConfiguration;
import io.kubernetes.client.openapi.models.V1MutatingWebhookConfigurationBuilder;
import io.kubernetes.client.openapi.models.V1RuleWithOperations;
import io.kubernetes.client.openapi.models.V1RuleWithOperationsBuilder;
import io.ten1010.aipub.projectcontroller.controller.AbstractReconciler;
import io.ten1010.aipub.projectcontroller.domain.k8s.DynamicCrConstants;
import io.ten1010.aipub.projectcontroller.domain.k8s.K8sApiProvider;
import io.ten1010.aipub.projectcontroller.domain.k8s.ProjectApiConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * user-label 낙인 웹훅(MutatingWebhookConfiguration)의 rules 를 CRD 목록에 맞춰 유지한다.
 * 고정 base rules(네이티브 리소스 + aipub 그룹)에 더해, 존재하는 CRD 그룹마다
 * scope "*" 의 CREATE 인터셉트 rule 을 생성하여 임의 CR(클러스터 스코프 포함) 생성 시
 * 소유자 레이블이 찍히도록 한다. 와일드카드로 전 그룹을 여는 대신 CRD 그룹만 정확히
 * 인터셉트하여, failurePolicy Fail 상태에서도 네이티브 클러스터 리소스(Node 등록 등)에
 * 영향을 주지 않는다.
 */
public class UserLabelWebhookConfigurationReconciler extends AbstractReconciler {

  public static final String WEBHOOK_CONFIGURATION_NAME =
      "userrelationship-v2.project-controller.project.aipub.ten1010.io";

  private final Indexer<V1MutatingWebhookConfiguration> webhookConfigurationIndexer;
  private final Indexer<V1CustomResourceDefinition> crdIndexer;
  private final AdmissionregistrationV1Api admissionregistrationV1Api;

  public UserLabelWebhookConfigurationReconciler(
      SharedInformerFactory sharedInformerFactory,
      K8sApiProvider k8sApiProvider) {
    this.webhookConfigurationIndexer = sharedInformerFactory
        .getExistingSharedIndexInformer(V1MutatingWebhookConfiguration.class)
        .getIndexer();
    this.crdIndexer = sharedInformerFactory
        .getExistingSharedIndexInformer(V1CustomResourceDefinition.class)
        .getIndexer();
    this.admissionregistrationV1Api = new AdmissionregistrationV1Api(
        k8sApiProvider.getApiClient());
  }

  @Override
  protected Result reconcileInternal(Request request) throws ApiException {
    if (!WEBHOOK_CONFIGURATION_NAME.equals(request.getName())) {
      return new Result(false);
    }
    V1MutatingWebhookConfiguration config = this.webhookConfigurationIndexer
        .getByKey(WEBHOOK_CONFIGURATION_NAME);
    if (config == null || config.getWebhooks() == null || config.getWebhooks().isEmpty()) {
      return new Result(false);
    }
    List<V1RuleWithOperations> reconciledRules = buildDesiredRules(this.crdIndexer.list());
    if (reconciledRules.equals(config.getWebhooks().get(0).getRules())) {
      return new Result(false);
    }
    V1MutatingWebhookConfiguration edited = new V1MutatingWebhookConfigurationBuilder(config)
        .editFirstWebhook()
        .withRules(reconciledRules)
        .endWebhook()
        .build();
    this.admissionregistrationV1Api
        .replaceMutatingWebhookConfiguration(WEBHOOK_CONFIGURATION_NAME, edited)
        .execute();
    return new Result(false);
  }

  static List<V1RuleWithOperations> buildDesiredRules(List<V1CustomResourceDefinition> crds) {
    List<V1RuleWithOperations> rules = new ArrayList<>(baseRules());
    crds.stream()
        .map(crd -> crd.getSpec() == null ? null : crd.getSpec().getGroup())
        .filter(Objects::nonNull)
        .filter(group -> !DynamicCrConstants.EXCLUDED_GROUPS.contains(group))
        .distinct()
        .sorted()
        .forEach(group -> rules.add(new V1RuleWithOperationsBuilder()
            .withApiGroups(group)
            .withApiVersions("*")
            .withOperations("CREATE")
            .withResources("*")
            .withScope("*")
            .build()));
    return rules;
  }

  /**
   * templates/mutating-webhook-user-v2.yaml 의 부트스트랩 rules 와 동일해야 한다.
   */
  private static List<V1RuleWithOperations> baseRules() {
    return List.of(
        new V1RuleWithOperationsBuilder()
            .withApiGroups("")
            .withApiVersions("*")
            .withOperations("CREATE")
            .withResources("pods", "replicationcontrollers", "services", "configmaps", "secrets",
                "persistentvolumeclaims")
            .withScope("Namespaced")
            .build(),
        new V1RuleWithOperationsBuilder()
            .withApiGroups("networking.k8s.io")
            .withApiVersions("*")
            .withOperations("CREATE")
            .withResources("ingresses")
            .withScope("Namespaced")
            .build(),
        new V1RuleWithOperationsBuilder()
            .withApiGroups("batch")
            .withApiVersions("*")
            .withOperations("CREATE")
            .withResources("*")
            .withScope("Namespaced")
            .build(),
        new V1RuleWithOperationsBuilder()
            .withApiGroups("apps")
            .withApiVersions("*")
            .withOperations("CREATE")
            .withResources("*")
            .withScope("Namespaced")
            .build(),
        new V1RuleWithOperationsBuilder()
            .withApiGroups(ProjectApiConstants.AIPUB_GROUP)
            .withApiVersions("*")
            .withOperations("CREATE")
            .withResources("*")
            .withScope("Namespaced")
            .build());
  }

}
