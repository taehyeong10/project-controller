package io.ten1010.aipub.projectcontroller.controller.rbac.aipub;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerWatch;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.workqueue.WorkQueue;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.models.V1ClusterRole;
import io.ten1010.aipub.projectcontroller.controller.ControllerFactory;
import io.ten1010.aipub.projectcontroller.controller.watch.DefaultControllerWatch;
import io.ten1010.aipub.projectcontroller.controller.watch.OnUpdateFilterFactory;
import io.ten1010.aipub.projectcontroller.controller.watch.RequestBuilderFactory;
import io.ten1010.aipub.projectcontroller.domain.k8s.K8sApiProvider;
import io.ten1010.aipub.projectcontroller.domain.k8s.ReconciliationService;
import io.ten1010.aipub.projectcontroller.domain.k8s.dto.V1alpha1AipubUser;
import io.ten1010.aipub.projectcontroller.domain.k8s.dto.V1alpha1Project;
import io.ten1010.aipub.projectcontroller.informer.dynamic.DynamicCrInformerManager;

public class AipubUserClusterRoleControllerFactory implements ControllerFactory {

  private final SharedInformerFactory sharedInformerFactory;
  private final OnUpdateFilterFactory onUpdateFilterFactory;
  private final RequestBuilderFactory requestBuilderFactory;
  private final K8sApiProvider k8sApiProvider;
  private final ReconciliationService reconciliationService;
  private final DynamicCrInformerManager dynamicCrInformerManager;

  public AipubUserClusterRoleControllerFactory(
      SharedInformerFactory sharedInformerFactory,
      K8sApiProvider k8sApiProvider,
      ReconciliationService reconciliationService,
      DynamicCrInformerManager dynamicCrInformerManager) {
    this.sharedInformerFactory = sharedInformerFactory;
    this.onUpdateFilterFactory = new OnUpdateFilterFactory();
    this.requestBuilderFactory = new RequestBuilderFactory(sharedInformerFactory);
    this.k8sApiProvider = k8sApiProvider;
    this.reconciliationService = reconciliationService;
    this.dynamicCrInformerManager = dynamicCrInformerManager;
  }

  @Override
  public Controller createController() {
    return ControllerBuilder.defaultBuilder(this.sharedInformerFactory)
        .withName("aipub-user-cluster-role-controller")
        .withWorkerCount(1)
        .withReadyFunc(this.sharedInformerFactory.getExistingSharedIndexInformer(
            V1ClusterRole.class)::hasSynced)
        .withReadyFunc(this.sharedInformerFactory.getExistingSharedIndexInformer(
            V1alpha1Project.class)::hasSynced)
        .watch(this::createClusterRoleWatch)
        .watch(this::createAipubUserWatch)
        .withReconciler(
            new AipubUserClusterRoleReconciler(this.sharedInformerFactory, this.k8sApiProvider,
                this.reconciliationService, this.dynamicCrInformerManager))
        .build();
  }

  private ControllerWatch<V1ClusterRole> createClusterRoleWatch(WorkQueue<Request> workQueue) {
    // 클러스터 스코프 동적 CR 이벤트가 이 컨트롤러의 큐로 직접 enqueue 되도록 큐를 등록한다
    this.dynamicCrInformerManager.setAipubUserClusterRoleWorkQueue(workQueue);
    DefaultControllerWatch<V1ClusterRole> watch = new DefaultControllerWatch<>(workQueue,
        V1ClusterRole.class);
    watch.setOnUpdateFilter(this.onUpdateFilterFactory.aipubUserClusterRoleFilter());
    return watch;
  }

  private ControllerWatch<V1alpha1AipubUser> createAipubUserWatch(WorkQueue<Request> workQueue) {
    DefaultControllerWatch<V1alpha1AipubUser> watch = new DefaultControllerWatch<>(workQueue,
        V1alpha1AipubUser.class);
    watch.setOnUpdateFilter(this.onUpdateFilterFactory.alwaysFalseFilter());
    watch.setRequestBuilder(this.requestBuilderFactory.aipubUserToClusterRoles());
    return watch;
  }

}
