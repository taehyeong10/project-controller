package io.ten1010.aipub.projectcontroller.controller.webhook;

import static org.assertj.core.api.Assertions.assertThat;

import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinitionBuilder;
import io.kubernetes.client.openapi.models.V1RuleWithOperations;
import java.util.List;
import org.junit.jupiter.api.Test;

class UserLabelWebhookConfigurationReconcilerTest {

  private static V1CustomResourceDefinition crd(String group) {
    return new V1CustomResourceDefinitionBuilder()
        .withNewSpec()
        .withGroup(group)
        .endSpec()
        .build();
  }

  @Test
  void buildDesiredRules_appendsCrdGroupRulesSorted() {
    List<V1RuleWithOperations> rules = UserLabelWebhookConfigurationReconciler.buildDesiredRules(
        List.of(crd("zeta.example.com"), crd("alpha.example.com"), crd("zeta.example.com")));

    List<V1RuleWithOperations> baseOnly = UserLabelWebhookConfigurationReconciler
        .buildDesiredRules(List.of());
    assertThat(rules).hasSize(baseOnly.size() + 2);

    V1RuleWithOperations alphaRule = rules.get(rules.size() - 2);
    assertThat(alphaRule.getApiGroups()).containsExactly("alpha.example.com");
    assertThat(alphaRule.getApiVersions()).containsExactly("*");
    assertThat(alphaRule.getOperations()).containsExactly("CREATE");
    assertThat(alphaRule.getResources()).containsExactly("*");
    assertThat(alphaRule.getScope()).isEqualTo("*");

    V1RuleWithOperations zetaRule = rules.get(rules.size() - 1);
    assertThat(zetaRule.getApiGroups()).containsExactly("zeta.example.com");
  }

  @Test
  void buildDesiredRules_excludesPlatformGroups() {
    List<V1RuleWithOperations> rules = UserLabelWebhookConfigurationReconciler.buildDesiredRules(
        List.of(crd("project.aipub.ten1010.io"), crd("aipub.ten1010.io"),
            crd("coaster.ten1010.io")));

    List<V1RuleWithOperations> baseOnly = UserLabelWebhookConfigurationReconciler
        .buildDesiredRules(List.of());
    assertThat(rules).isEqualTo(baseOnly);
  }

}
