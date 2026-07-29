package io.ten1010.aipub.projectcontroller.domain.k8s;

import static org.assertj.core.api.Assertions.assertThat;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PolicyRule;
import io.ten1010.aipub.projectcontroller.domain.k8s.dto.V1alpha1AipubUser;
import io.ten1010.aipub.projectcontroller.domain.k8s.dto.V1alpha1Project;
import io.ten1010.aipub.projectcontroller.domain.k8s.util.WorkloadExclusionResolver;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconciliationServiceOwnedCrRulesTest {

  private ReconciliationService reconciliationService;
  private V1alpha1AipubUser user;
  private V1alpha1Project project;

  @BeforeEach
  void setUp() {
    SubjectResolver subjectResolver = new SubjectResolver() {

      @Override
      public Optional<io.kubernetes.client.openapi.models.RbacV1Subject> resolve(
          io.ten1010.aipub.projectcontroller.domain.k8s.dto.V1alpha1ProjectMember member) {
        return Optional.empty();
      }

      @Override
      public Optional<io.kubernetes.client.openapi.models.RbacV1Subject> resolve(
          V1alpha1AipubUser user) {
        return Optional.empty();
      }

    };
    this.reconciliationService = new ReconciliationService(
        subjectResolver,
        project -> java.util.Map.of(),
        List.of(),
        new WorkloadExclusionResolver(List.of()));

    this.user = new V1alpha1AipubUser();
    V1ObjectMeta userMeta = new V1ObjectMeta();
    userMeta.setName("alice");
    this.user.setMetadata(userMeta);

    this.project = new V1alpha1Project();
    V1ObjectMeta projectMeta = new V1ObjectMeta();
    projectMeta.setName("proj1");
    this.project.setMetadata(projectMeta);
  }

  @Test
  void reconcileAipubUserRoleRules_withOwnedCrObjects_appendsOwnedCrRules() {
    List<V1PolicyRule> rules = this.reconciliationService.reconcileAipubUserRoleRules(
        this.user, this.project, List.of(),
        List.of(new OwnedCrObject("example.com", "widgets", "my-widget")));

    assertThat(rules).hasSize(1);
    V1PolicyRule rule = rules.get(0);
    assertThat(rule.getApiGroups()).containsExactly("example.com");
    assertThat(rule.getResources()).containsExactly("widgets");
    assertThat(rule.getResourceNames()).containsExactly("my-widget");
    assertThat(rule.getVerbs()).containsExactly("get", "update", "patch", "delete");
  }

  @Test
  void reconcileAipubUserRoleRules_withoutOwnedCrObjects_returnsSameAsLegacyOverload() {
    List<V1PolicyRule> legacy = this.reconciliationService.reconcileAipubUserRoleRules(
        this.user, this.project, List.of());
    List<V1PolicyRule> rules = this.reconciliationService.reconcileAipubUserRoleRules(
        this.user, this.project, List.of(), List.of());

    assertThat(rules).isEqualTo(legacy);
  }

  @Test
  void reconcileClusterRoleRules_withOwnedCrObjects_appendsOwnedCrRules() {
    List<V1PolicyRule> baseRules = this.reconciliationService.reconcileClusterRoleRules(this.user);
    List<V1PolicyRule> rules = this.reconciliationService.reconcileClusterRoleRules(
        this.user,
        List.of(new OwnedCrObject("example.com", "clusterwidgets", "my-cluster-widget")));

    assertThat(rules).hasSize(baseRules.size() + 1);
    assertThat(rules.subList(0, baseRules.size())).isEqualTo(baseRules);
    V1PolicyRule appended = rules.get(rules.size() - 1);
    assertThat(appended.getApiGroups()).containsExactly("example.com");
    assertThat(appended.getResources()).containsExactly("clusterwidgets");
    assertThat(appended.getResourceNames()).containsExactly("my-cluster-widget");
    assertThat(appended.getVerbs()).containsExactly("get", "update", "patch", "delete");
  }

}
