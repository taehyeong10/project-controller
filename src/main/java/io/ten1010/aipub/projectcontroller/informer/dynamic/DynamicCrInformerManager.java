package io.ten1010.aipub.projectcontroller.informer.dynamic;

import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.workqueue.WorkQueue;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Pair;
import io.kubernetes.client.openapi.models.V1ClusterRole;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinitionSpec;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinitionVersion;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.util.CallGeneratorParams;
import io.ten1010.aipub.projectcontroller.domain.k8s.AipubUserRoleNameResolver;
import io.ten1010.aipub.projectcontroller.domain.k8s.DynamicCrConstants;
import io.ten1010.aipub.projectcontroller.domain.k8s.LabelConstants;
import io.ten1010.aipub.projectcontroller.domain.k8s.OwnedCrObject;
import io.ten1010.aipub.projectcontroller.domain.k8s.ResourceTarget;
import io.ten1010.aipub.projectcontroller.domain.k8s.dto.V1PartialObject;
import io.ten1010.aipub.projectcontroller.domain.k8s.dto.V1PartialObjectList;
import io.ten1010.aipub.projectcontroller.domain.k8s.util.K8sObjectUtils;
import io.ten1010.aipub.projectcontroller.domain.k8s.util.UsernameUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import org.jspecify.annotations.Nullable;

/**
 * 소유권 추적 대상 리소스 타입마다 username 레이블 셀렉터가 걸린 메타데이터 전용 인포머를
 * 기동/중지하고, 소유 오브젝트 변화를 개인 Role/ClusterRole 리컨실 큐로 라우팅한다.
 * 대상은 두 종류다: CRD 워치로 발견되는 임의 CR 타입(생성/삭제에 따라 동적 기동/중지)과
 * {@link DynamicCrConstants#NATIVE_OWNED_TARGETS} 의 네이티브 타입(기동 시 상시 추적).
 * 리컨실러는 이 매니저를 통해 사용자별 소유 오브젝트 목록을 조회하여 resourceNames 기반
 * RBAC 규칙을 생성한다.
 */
@Slf4j
public class DynamicCrInformerManager {

  private record TrackedTarget(ResourceTarget target, SharedInformerFactory informerFactory,
      SharedIndexInformer<V1PartialObject> informer) {
  }

  private final ApiClient apiClient;
  private final AipubUserRoleNameResolver roleNameResolver;
  private final Indexer<V1Role> roleIndexer;
  private final Indexer<V1ClusterRole> clusterRoleIndexer;
  private final Map<String, TrackedTarget> trackedTargets;

  @Setter
  private volatile WorkQueue<Request> aipubUserRoleWorkQueue;
  @Setter
  private volatile WorkQueue<Request> aipubUserClusterRoleWorkQueue;

  public DynamicCrInformerManager(ApiClient apiClient,
      SharedInformerFactory sharedInformerFactory) {
    this.apiClient = apiClient;
    this.roleNameResolver = new AipubUserRoleNameResolver();
    this.roleIndexer = sharedInformerFactory
        .getExistingSharedIndexInformer(V1Role.class)
        .getIndexer();
    this.clusterRoleIndexer = sharedInformerFactory
        .getExistingSharedIndexInformer(V1ClusterRole.class)
        .getIndexer();
    this.trackedTargets = new ConcurrentHashMap<>();
    for (ResourceTarget nativeTarget : DynamicCrConstants.NATIVE_OWNED_TARGETS) {
      startTracking(trackingKey(nativeTarget.group(), nativeTarget.plural()), nativeTarget);
    }
    sharedInformerFactory.getExistingSharedIndexInformer(V1CustomResourceDefinition.class)
        .addEventHandler(new ResourceEventHandler<>() {

          @Override
          public void onAdd(V1CustomResourceDefinition obj) {
            track(obj);
          }

          @Override
          public void onUpdate(V1CustomResourceDefinition oldObj,
              V1CustomResourceDefinition newObj) {
            track(newObj);
          }

          @Override
          public void onDelete(V1CustomResourceDefinition obj, boolean deletedFinalStateUnknown) {
            untrack(obj);
          }

        });
  }

  public List<OwnedCrObject> getNamespacedOwnedObjects(String namespace, String aipubUserName) {
    return getOwnedObjects(true, obj -> namespace.equals(resolveNamespace(obj)), aipubUserName);
  }

  public List<OwnedCrObject> getClusterOwnedObjects(String aipubUserName) {
    return getOwnedObjects(false, obj -> true, aipubUserName);
  }

  private List<OwnedCrObject> getOwnedObjects(boolean namespaced,
      Predicate<V1PartialObject> filter, String aipubUserName) {
    List<OwnedCrObject> owned = new ArrayList<>();
    for (TrackedTarget tracked : this.trackedTargets.values()) {
      if (tracked.target().namespaced() != namespaced) {
        continue;
      }
      for (V1PartialObject obj : tracked.informer().getIndexer().list()) {
        if (!filter.test(obj)) {
          continue;
        }
        if (UsernameUtils.getUsername(obj).filter(aipubUserName::equals).isEmpty()) {
          continue;
        }
        owned.add(new OwnedCrObject(tracked.target().group(), tracked.target().plural(),
            K8sObjectUtils.getName(obj)));
      }
    }
    // reconcileExistingRole 이 규칙 리스트를 equals 로 비교하므로 순서가 결정적이어야 한다
    owned.sort(Comparator.comparing(OwnedCrObject::group)
        .thenComparing(OwnedCrObject::resource)
        .thenComparing(OwnedCrObject::name));
    return owned;
  }

  static Optional<ResourceTarget> resolveTarget(V1CustomResourceDefinition crd) {
    V1CustomResourceDefinitionSpec spec = crd.getSpec();
    if (spec == null || spec.getGroup() == null || spec.getNames() == null
        || spec.getNames().getPlural() == null) {
      return Optional.empty();
    }
    if (DynamicCrConstants.EXCLUDED_GROUPS.contains(spec.getGroup())) {
      return Optional.empty();
    }
    List<V1CustomResourceDefinitionVersion> versions =
        spec.getVersions() == null ? List.of() : spec.getVersions();
    Optional<String> versionOpt = versions.stream()
        .filter(v -> Boolean.TRUE.equals(v.getStorage()))
        .map(V1CustomResourceDefinitionVersion::getName)
        .findFirst()
        .or(() -> versions.stream()
            .filter(v -> Boolean.TRUE.equals(v.getServed()))
            .map(V1CustomResourceDefinitionVersion::getName)
            .findFirst());
    return versionOpt.map(version -> new ResourceTarget(
        spec.getGroup(),
        version,
        spec.getNames().getPlural(),
        "Namespaced".equals(spec.getScope())));
  }

  private static String trackingKey(String group, String plural) {
    return group + "/" + plural;
  }

  private synchronized void track(V1CustomResourceDefinition crd) {
    Optional<ResourceTarget> targetOpt = resolveTarget(crd);
    if (targetOpt.isEmpty()) {
      return;
    }
    ResourceTarget target = targetOpt.get();
    String key = trackingKey(target.group(), target.plural());
    TrackedTarget existing = this.trackedTargets.get(key);
    if (existing != null) {
      if (existing.target().equals(target)) {
        return;
      }
      stopTracking(key, existing);
    }
    startTracking(key, target);
  }

  private synchronized void untrack(V1CustomResourceDefinition crd) {
    V1CustomResourceDefinitionSpec spec = crd.getSpec();
    if (spec == null || spec.getGroup() == null || spec.getNames() == null
        || spec.getNames().getPlural() == null) {
      return;
    }
    String key = trackingKey(spec.getGroup(), spec.getNames().getPlural());
    TrackedTarget tracked = this.trackedTargets.get(key);
    if (tracked == null) {
      return;
    }
    stopTracking(key, tracked);
  }

  private synchronized void startTracking(String key, ResourceTarget target) {
    SharedInformerFactory informerFactory = new SharedInformerFactory(this.apiClient);
    SharedIndexInformer<V1PartialObject> informer = informerFactory.sharedIndexInformerFor(
        (CallGeneratorParams params) -> buildListCall(target, params),
        V1PartialObject.class,
        V1PartialObjectList.class);
    informer.addEventHandler(new ResourceEventHandler<>() {

      @Override
      public void onAdd(V1PartialObject obj) {
        enqueueOwnerRole(target, obj);
      }

      @Override
      public void onUpdate(V1PartialObject oldObj, V1PartialObject newObj) {
        enqueueOwnerRole(target, oldObj);
        enqueueOwnerRole(target, newObj);
      }

      @Override
      public void onDelete(V1PartialObject obj, boolean deletedFinalStateUnknown) {
        enqueueOwnerRole(target, obj);
      }

    });
    this.trackedTargets.put(key, new TrackedTarget(target, informerFactory, informer));
    informerFactory.startAllRegisteredInformers();
    log.info("Started owned-object informer: group={}, version={}, plural={}, namespaced={}",
        target.group(), target.version(), target.plural(), target.namespaced());
  }

  private void stopTracking(String key, TrackedTarget tracked) {
    this.trackedTargets.remove(key);
    tracked.informerFactory().stopAllRegisteredInformers();
    enqueueAllPersonalRoles(tracked.target().namespaced());
    log.info("Stopped owned-object informer: {}", key);
  }

  /**
   * core("") 그룹은 /api/v1, 나머지는 /apis/{group}/{version} 경로를 사용한다.
   * username 레이블이 있는 오브젝트만 list/watch 한다.
   */
  private Call buildListCall(ResourceTarget target, CallGeneratorParams params) {
    String basePath = target.group().isEmpty()
        ? "/api/" + target.version()
        : "/apis/" + target.group() + "/" + target.version();
    String path = basePath + "/" + target.plural();

    List<Pair> queryParams = new ArrayList<>();
    queryParams.add(new Pair("labelSelector", LabelConstants.OBJECT_OWN_USERNAME_KEY));
    if (params.resourceVersion != null) {
      queryParams.add(new Pair("resourceVersion", params.resourceVersion));
    }
    if (params.watch != null) {
      queryParams.add(new Pair("watch", String.valueOf(params.watch)));
    }
    if (params.timeoutSeconds != null) {
      queryParams.add(new Pair("timeoutSeconds", String.valueOf(params.timeoutSeconds)));
    }

    try {
      return this.apiClient.buildCall(
          this.apiClient.getBasePath(), path, "GET",
          queryParams, List.of(),
          null,
          Map.of(), Map.of(), Map.of(),
          new String[]{"BearerToken"}, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build list call: " + path, e);
    }
  }

  private void enqueueOwnerRole(ResourceTarget target, V1PartialObject obj) {
    Optional<String> usernameOpt = UsernameUtils.getUsername(obj);
    if (usernameOpt.isEmpty()) {
      return;
    }
    String roleName;
    try {
      roleName = this.roleNameResolver.resolveRoleName(usernameOpt.get());
    } catch (IllegalArgumentException e) {
      log.warn("Invalid username label value: {}", usernameOpt.get());
      return;
    }
    if (target.namespaced()) {
      WorkQueue<Request> queue = this.aipubUserRoleWorkQueue;
      String namespace = resolveNamespace(obj);
      if (queue == null || namespace == null) {
        return;
      }
      queue.add(new Request(namespace, roleName));
    } else {
      WorkQueue<Request> queue = this.aipubUserClusterRoleWorkQueue;
      if (queue == null) {
        return;
      }
      queue.add(new Request(roleName));
    }
  }

  private void enqueueAllPersonalRoles(boolean namespaced) {
    if (namespaced) {
      WorkQueue<Request> queue = this.aipubUserRoleWorkQueue;
      if (queue == null) {
        return;
      }
      for (V1Role role : this.roleIndexer.list()) {
        String name = K8sObjectUtils.getName(role);
        if (this.roleNameResolver.resolveAipubUserName(name).isEmpty()) {
          continue;
        }
        queue.add(new Request(K8sObjectUtils.getNamespace(role), name));
      }
    } else {
      WorkQueue<Request> queue = this.aipubUserClusterRoleWorkQueue;
      if (queue == null) {
        return;
      }
      for (V1ClusterRole role : this.clusterRoleIndexer.list()) {
        String name = K8sObjectUtils.getName(role);
        if (this.roleNameResolver.resolveAipubUserName(name).isEmpty()) {
          continue;
        }
        queue.add(new Request(name));
      }
    }
  }

  @Nullable
  private static String resolveNamespace(V1PartialObject obj) {
    return obj.getMetadata() == null ? null : obj.getMetadata().getNamespace();
  }

}
