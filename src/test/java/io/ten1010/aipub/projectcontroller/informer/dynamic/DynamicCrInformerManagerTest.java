package io.ten1010.aipub.projectcontroller.informer.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinitionBuilder;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DynamicCrInformerManagerTest {

  private static V1CustomResourceDefinition crd(String group, String plural, String scope,
      boolean storageServed) {
    return new V1CustomResourceDefinitionBuilder()
        .withNewMetadata()
        .withName(plural + "." + group)
        .endMetadata()
        .withNewSpec()
        .withGroup(group)
        .withScope(scope)
        .withNewNames()
        .withKind("Widget")
        .withPlural(plural)
        .endNames()
        .addNewVersion()
        .withName("v1alpha1")
        .withServed(true)
        .withStorage(false)
        .endVersion()
        .addNewVersion()
        .withName("v1")
        .withServed(storageServed)
        .withStorage(true)
        .endVersion()
        .endSpec()
        .build();
  }

  @Test
  void resolveTarget_picksStorageVersion() {
    Optional<DynamicCrInformerManager.CrdTarget> target =
        DynamicCrInformerManager.resolveTarget(crd("example.com", "widgets", "Namespaced", true));

    assertThat(target).isPresent();
    assertThat(target.get().group()).isEqualTo("example.com");
    assertThat(target.get().version()).isEqualTo("v1");
    assertThat(target.get().plural()).isEqualTo("widgets");
    assertThat(target.get().namespaced()).isTrue();
  }

  @Test
  void resolveTarget_clusterScope_namespacedFalse() {
    Optional<DynamicCrInformerManager.CrdTarget> target =
        DynamicCrInformerManager.resolveTarget(crd("example.com", "widgets", "Cluster", true));

    assertThat(target).isPresent();
    assertThat(target.get().namespaced()).isFalse();
  }

  @Test
  void resolveTarget_excludedGroups_returnsEmpty() {
    assertThat(DynamicCrInformerManager.resolveTarget(
        crd("project.aipub.ten1010.io", "projects", "Cluster", true))).isEmpty();
    assertThat(DynamicCrInformerManager.resolveTarget(
        crd("aipub.ten1010.io", "workspaces", "Namespaced", true))).isEmpty();
    assertThat(DynamicCrInformerManager.resolveTarget(
        crd("coaster.ten1010.io", "noderesources", "Cluster", true))).isEmpty();
  }

  @Test
  void resolveTarget_missingSpecFields_returnsEmpty() {
    V1CustomResourceDefinition crd = new V1CustomResourceDefinition();
    assertThat(DynamicCrInformerManager.resolveTarget(crd)).isEmpty();
  }

}
