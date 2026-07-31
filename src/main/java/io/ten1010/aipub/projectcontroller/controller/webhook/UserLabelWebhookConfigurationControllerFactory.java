package io.ten1010.aipub.projectcontroller.controller.webhook;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerWatch;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.workqueue.WorkQueue;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;
import io.kubernetes.client.openapi.models.V1MutatingWebhookConfiguration;
import io.ten1010.aipub.projectcontroller.controller.ControllerFactory;
import io.ten1010.aipub.projectcontroller.controller.watch.DefaultControllerWatch;
import io.ten1010.aipub.projectcontroller.domain.k8s.K8sApiProvider;
import java.util.List;

public class UserLabelWebhookConfigurationControllerFactory implements ControllerFactory {

  private final SharedInformerFactory sharedInformerFactory;
  private final K8sApiProvider k8sApiProvider;

  public UserLabelWebhookConfigurationControllerFactory(
      SharedInformerFactory sharedInformerFactory,
      K8sApiProvider k8sApiProvider) {
    this.sharedInformerFactory = sharedInformerFactory;
    this.k8sApiProvider = k8sApiProvider;
  }

  @Override
  public Controller createController() {
    return ControllerBuilder.defaultBuilder(this.sharedInformerFactory)
        .withName("user-label-webhook-configuration-controller")
        .withWorkerCount(1)
        .withReadyFunc(this.sharedInformerFactory.getExistingSharedIndexInformer(
            V1MutatingWebhookConfiguration.class)::hasSynced)
        .withReadyFunc(this.sharedInformerFactory.getExistingSharedIndexInformer(
            V1CustomResourceDefinition.class)::hasSynced)
        .watch(this::createWebhookConfigurationWatch)
        .watch(this::createCustomResourceDefinitionWatch)
        .withReconciler(new UserLabelWebhookConfigurationReconciler(this.sharedInformerFactory,
            this.k8sApiProvider))
        .build();
  }

  private ControllerWatch<V1MutatingWebhookConfiguration> createWebhookConfigurationWatch(
      WorkQueue<Request> workQueue) {
    return new DefaultControllerWatch<>(workQueue, V1MutatingWebhookConfiguration.class);
  }

  private ControllerWatch<V1CustomResourceDefinition> createCustomResourceDefinitionWatch(
      WorkQueue<Request> workQueue) {
    DefaultControllerWatch<V1CustomResourceDefinition> watch = new DefaultControllerWatch<>(
        workQueue, V1CustomResourceDefinition.class);
    watch.setRequestBuilder(obj -> List.of(
        new Request(UserLabelWebhookConfigurationReconciler.WEBHOOK_CONFIGURATION_NAME)));
    return watch;
  }

}
