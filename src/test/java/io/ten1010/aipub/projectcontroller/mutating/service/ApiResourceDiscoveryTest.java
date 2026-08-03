package io.ten1010.aipub.projectcontroller.mutating.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import io.kubernetes.client.openapi.ApiClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ApiResourceDiscoveryTest {

  private ApiClient mockApiClient;
  private ApiResourceDiscovery discovery;

  @BeforeEach
  void setUp() throws Exception {
    this.mockApiClient = mock(ApiClient.class);
    when(this.mockApiClient.getBasePath()).thenReturn("https://localhost:6443");

    // buildSnapshot() 호출 시 /api/v1, /apis 응답 mock
    // /api/v1 — core resources: pods(namespaced), nodes(cluster-scoped)
    mockApiCall("/api/v1", """
        {
          "resources": [
            {"name": "pods", "kind": "Pod", "namespaced": true},
            {"name": "nodes", "kind": "Node", "namespaced": false},
            {"name": "services", "kind": "Service", "namespaced": true}
          ]
        }
        """);

    // /apis — apps/v1 group
    mockApiCall("/apis", """
        {
          "groups": [
            {
              "name": "apps",
              "versions": [{"groupVersion": "apps/v1"}]
            }
          ]
        }
        """);

    mockApiCall("/apis/apps/v1", """
        {
          "resources": [
            {"name": "deployments", "kind": "Deployment", "namespaced": true}
          ]
        }
        """);

    this.discovery = new ApiResourceDiscovery(this.mockApiClient);
  }

  private void mockApiCall(String path, String responseBody) throws Exception {
    Call call = mock(Call.class);
    Response response = new Response.Builder()
        .request(new Request.Builder().url("https://localhost:6443" + path).build())
        .protocol(Protocol.HTTP_1_1)
        .code(200)
        .message("OK")
        .body(ResponseBody.create(responseBody, MediaType.get("application/json")))
        .build();
    when(call.execute()).thenReturn(response);
    when(this.mockApiClient.buildCall(
        any(), eq(path), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(call);
  }

  // === buildSnapshot — booleanValue 테스트 ===

  @Nested
  class BooleanValueBehavior {

    // booleanValue()는 JSON boolean만 true로 인식.
    // 수정 전 asBoolean()은 문자열 "true"도 true로 변환했음.
    @Test
    void namespacedField_trueBoolean_isNamespacedReturnsTrue() {
      // pods는 namespaced: true (boolean)
      assertThat(discovery.isNamespaced("/pods")).isTrue();
    }

    @Test
    void namespacedField_falseBoolean_isNamespacedReturnsFalse() {
      // nodes는 namespaced: false (boolean)
      assertThat(discovery.isNamespaced("/nodes")).isFalse();
    }

    @Test
    void namespacedFieldMissing_booleanValueReturnsFalse() throws Exception {
      // "namespaced" 필드가 없는 리소스 → booleanValue()는 false 반환
      // asBoolean()도 false 반환하지만, booleanValue()는 명시적 boolean만 처리
      ApiClient client = mock(ApiClient.class);
      when(client.getBasePath()).thenReturn("https://localhost:6443");

      mockApiCallForClient(client, "/api/v1", """
          {
            "resources": [
              {"name": "testresources", "kind": "TestResource"}
            ]
          }
          """);
      mockApiCallForClient(client, "/apis", """
          {"groups": []}
          """);

      ApiResourceDiscovery disc = new ApiResourceDiscovery(client);
      // namespaced 필드 누락 → false로 분류 (cluster-scoped 취급)
      assertThat(disc.isNamespaced("/testresources")).isFalse();
    }

    private void mockApiCallForClient(ApiClient client, String path, String body)
        throws Exception {
      Call call = mock(Call.class);
      Response response = new Response.Builder()
          .request(new Request.Builder().url("https://localhost:6443" + path).build())
          .protocol(Protocol.HTTP_1_1)
          .code(200)
          .message("OK")
          .body(ResponseBody.create(body, MediaType.get("application/json")))
          .build();
      when(call.execute()).thenReturn(response);
      when(client.buildCall(
          any(), eq(path), any(), any(), any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(call);
    }
  }

  // === getAllObjectNames — "core" group alias 테스트 ===

  @Nested
  class CoreGroupAlias {

    // 수정 후: "core" group도 /api/v1/ 경로 사용
    // 수정 전: "core"는 non-core 경로로 처리되어 groupVersion 조회 실패 → RuntimeException
    @Test
    void emptyGroup_usesCorePath() throws Exception {
      // "/pods" → /api/v1/pods
      mockApiCall("/api/v1/pods", """
          {
            "items": [
              {"metadata": {"name": "pod-1"}},
              {"metadata": {"name": "pod-2"}}
            ]
          }
          """);

      List<String> names = discovery.getAllObjectNames("/pods", null);
      assertThat(names).containsExactly("pod-1", "pod-2");
    }

    @Test
    void coreGroup_usesCorePath() throws Exception {
      // "core/pods" → /api/v1/pods (수정 후 동작)
      mockApiCall("/api/v1/pods", """
          {
            "items": [
              {"metadata": {"name": "pod-1"}},
              {"metadata": {"name": "pod-2"}}
            ]
          }
          """);

      List<String> names = discovery.getAllObjectNames("core/pods", null);
      assertThat(names).containsExactly("pod-1", "pod-2");
    }

    @Test
    void emptyGroup_withNamespace_usesNamespacedCorePath() throws Exception {
      // "/pods" + namespace "ns1" → /api/v1/namespaces/ns1/pods
      mockApiCall("/api/v1/namespaces/ns1/pods", """
          {
            "items": [
              {"metadata": {"name": "ns1-pod"}}
            ]
          }
          """);

      List<String> names = discovery.getAllObjectNames("/pods", "ns1");
      assertThat(names).containsExactly("ns1-pod");
    }

    // "core/pods"는 snapshot에 "/pods"로 저장되므로
    // isNamespaced("core/pods") 호출 시 GroupResourceNotFoundException 발생.
    // Python도 동일 동작 — core alias는 namespace 없는 경우에만 유효.
    @Test
    void coreGroup_withNamespace_throwsBecauseNotInSnapshot() {
      assertThatThrownBy(() -> discovery.getAllObjectNames("core/pods", "ns1"))
          .isInstanceOf(GroupResourceNotFoundException.class);
    }

    @Test
    void nonCoreGroup_usesApisPath() throws Exception {
      // "apps/deployments" → /apis/apps/v1/deployments
      mockApiCall("/apis/apps/v1/deployments", """
          {
            "items": [
              {"metadata": {"name": "deploy-1"}}
            ]
          }
          """);

      List<String> names = discovery.getAllObjectNames("apps/deployments", null);
      assertThat(names).containsExactly("deploy-1");
    }

    @Test
    void nonCoreGroup_unknownGroupVersion_throwsException() {
      // "unknown/resources" → groupVersion 없음 → RuntimeException
      assertThatThrownBy(() -> discovery.getAllObjectNames("unknown/resources", null))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Unknown groupVersion");
    }
  }

  // === getResourcesByKind — core groupVersions 버그 수정 검증 ===

  @Nested
  class GetResourcesByKind {

    // 수정 전: core 루프가 groupVersions를 채우지 않아 core kind는 항상 빈 목록 반환.
    // 수정 후: core groupResource("/pods" 등)에 "v1"이 기록되어 ResourceInfo("v1", plural) 반환.
    @Test
    void coreKind_returnsV1Resource() {
      assertThat(discovery.getResourcesByKind("Pod"))
          .containsExactly(new ApiResourceDiscovery.ResourceInfo("v1", "pods"));
    }

    @Test
    void coreKind_namespacedService_returnsV1Resource() {
      assertThat(discovery.getResourcesByKind("Service"))
          .containsExactly(new ApiResourceDiscovery.ResourceInfo("v1", "services"));
    }

    @Test
    void coreKind_clusterScoped_returnsV1Resource() {
      assertThat(discovery.getResourcesByKind("Node"))
          .containsExactly(new ApiResourceDiscovery.ResourceInfo("v1", "nodes"));
    }

    @Test
    void nonCoreKind_returnsGroupVersionResource() {
      assertThat(discovery.getResourcesByKind("Deployment"))
          .containsExactly(new ApiResourceDiscovery.ResourceInfo("apps/v1", "deployments"));
    }

    @Test
    void unknownKind_returnsEmptyList() {
      assertThat(discovery.getResourcesByKind("Unknown")).isEmpty();
    }

    @Test
    void coreGroupResource_getGroupVersion_returnsV1() {
      assertThat(discovery.getGroupVersion("/pods")).isEqualTo("v1");
    }
  }

  // === isExist / isNamespaced 기본 동작 ===

  @Nested
  class BasicDiscovery {

    @Test
    void coreResource_exists() {
      assertThat(discovery.isExist("/pods")).isTrue();
      assertThat(discovery.isExist("/nodes")).isTrue();
    }

    @Test
    void nonCoreResource_exists() {
      assertThat(discovery.isExist("apps/deployments")).isTrue();
    }

    @Test
    void unknownResource_doesNotExist() {
      assertThat(discovery.isExist("unknown/resources")).isFalse();
    }

    // "core/pods"는 snapshot에 "/pods"로 저장되므로 isExist는 false
    // Python도 동일 동작
    @Test
    void coreAliasResource_doesNotExist() {
      assertThat(discovery.isExist("core/pods")).isFalse();
    }
  }

  // === refresh-on-miss — 스냅샷 미스 시 targeted refresh ===

  @Nested
  class RefreshOnMiss {

    // (a) 스냅샷 미스 → 해당 그룹만 targeted refresh → 히트.
    // CRD 생성 직후 5분 주기 전체 refresh 전에 들어온 웹훅 요청이 400 거부되던 문제의 재현/수정 검증.
    @Test
    void missedGroup_targetedRefresh_thenHit() throws Exception {
      mockApiCallRepeatable("/apis/qa-collision.ten1010.io", """
          {
            "kind": "APIGroup",
            "name": "qa-collision.ten1010.io",
            "versions": [
              {"groupVersion": "qa-collision.ten1010.io/v1", "version": "v1"},
              {"groupVersion": "qa-collision.ten1010.io/v2", "version": "v2"}
            ]
          }
          """);
      mockApiCallRepeatable("/apis/qa-collision.ten1010.io/v1", """
          {
            "resources": [
              {"name": "qamultiversions", "kind": "QaMultiVersion", "namespaced": true}
            ]
          }
          """);
      mockApiCallRepeatable("/apis/qa-collision.ten1010.io/v2", """
          {
            "resources": [
              {"name": "qamultiversions", "kind": "QaMultiVersion", "namespaced": true}
            ]
          }
          """);

      assertThat(discovery.isExist("qa-collision.ten1010.io/qamultiversions")).isTrue();
      assertThat(discovery.isNamespaced("qa-collision.ten1010.io/qamultiversions")).isTrue();
      assertThat(discovery.getGroupVersion("qa-collision.ten1010.io/qamultiversions"))
          .isEqualTo("qa-collision.ten1010.io/v2");
      // 병합이 기존 스냅샷 항목을 덮어쓰지 않음
      assertThat(discovery.isExist("/pods")).isTrue();
      assertThat(discovery.isExist("apps/deployments")).isTrue();
      assertThat(discovery.getResourcesByKind("Deployment"))
          .containsExactly(new ApiResourceDiscovery.ResourceInfo("apps/v1", "deployments"));
      // 첫 miss의 targeted refresh 이후에는 스냅샷 히트 → 그룹 재조회 없음
      verifyBuildCallCount("/apis/qa-collision.ten1010.io", 1);
      verifyBuildCallCount("/apis/qa-collision.ten1010.io/v1", 1);
      verifyBuildCallCount("/apis/qa-collision.ten1010.io/v2", 1);
    }

    // (b) API 서버에도 없는 그룹 → miss + 그룹 negative cache. TTL 내 동일 그룹 재조회 없음.
    @Test
    void missingGroup_negativeCached_noRepeatedApiCalls() throws Exception {
      mockApiCallNotFound("/apis/ghost.example.io");

      assertThat(discovery.isExist("ghost.example.io/things")).isFalse();
      // TTL 내 동일 그룹의 어떤 리소스든 API 호출 없이 즉시 miss
      assertThat(discovery.isExist("ghost.example.io/things")).isFalse();
      assertThat(discovery.isExist("ghost.example.io/others")).isFalse();
      assertThatThrownBy(() -> discovery.isNamespaced("ghost.example.io/things"))
          .isInstanceOf(GroupResourceNotFoundException.class);

      verifyBuildCallCount("/apis/ghost.example.io", 1);
    }

    // (c) 동일 그룹 동시 미스 → in-flight refresh 하나를 공유, API 호출 1회
    @Test
    void concurrentMisses_shareSingleInFlightRefresh() throws Exception {
      CountDownLatch releaseLatch = new CountDownLatch(1);
      Call groupCall = mock(Call.class);
      when(groupCall.execute()).thenAnswer(invocation -> {
        // 두 스레드 모두 in-flight future 대기에 진입할 때까지 응답을 지연
        releaseLatch.await(5, TimeUnit.SECONDS);
        return buildJsonResponse("/apis/newgroup.example.io", 200, """
            {
              "kind": "APIGroup",
              "name": "newgroup.example.io",
              "versions": [{"groupVersion": "newgroup.example.io/v1", "version": "v1"}]
            }
            """);
      });
      when(mockApiClient.buildCall(
          any(), eq("/apis/newgroup.example.io"), any(), any(), any(), any(), any(), any(), any(),
          any(), any()))
          .thenReturn(groupCall);
      mockApiCallRepeatable("/apis/newgroup.example.io/v1", """
          {
            "resources": [
              {"name": "newthings", "kind": "NewThing", "namespaced": true}
            ]
          }
          """);

      ExecutorService pool = Executors.newFixedThreadPool(2);
      try {
        CountDownLatch started = new CountDownLatch(2);
        List<Future<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
          results.add(pool.submit(() -> {
            started.countDown();
            return discovery.isExist("newgroup.example.io/newthings");
          }));
        }
        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);
        releaseLatch.countDown();

        assertThat(results.get(0).get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(results.get(1).get(5, TimeUnit.SECONDS)).isTrue();
      } finally {
        pool.shutdownNow();
      }

      verifyBuildCallCount("/apis/newgroup.example.io", 1);
      verifyBuildCallCount("/apis/newgroup.example.io/v1", 1);
    }

    // (d) 기존 히트 경로는 API 호출 없음
    @Test
    void hitPath_makesNoApiCalls() {
      clearInvocations(mockApiClient);

      assertThat(discovery.isExist("apps/deployments")).isTrue();
      assertThat(discovery.isNamespaced("apps/deployments")).isTrue();
      assertThat(discovery.isExist("/pods")).isTrue();
      assertThat(discovery.isNamespaced("/nodes")).isFalse();

      verifyNoBuildCall();
    }

    // 코어 그룹("", "core" alias)과 형식 오류의 미스는 targeted refresh 대상이 아님 — 기존 동작 유지
    @Test
    void coreOrMalformedMiss_doesNotTriggerTargetedRefresh() {
      clearInvocations(mockApiClient);

      assertThat(discovery.isExist("/ghosts")).isFalse();
      assertThat(discovery.isExist("core/ghosts")).isFalse();
      assertThat(discovery.isExist("noslash")).isFalse();
      assertThatThrownBy(() -> discovery.isNamespaced("/ghosts"))
          .isInstanceOf(GroupResourceNotFoundException.class);

      verifyNoBuildCall();
    }

    // 그룹은 존재하지만 요청 리소스가 없음 → miss + 리소스 단위 negative cache.
    // TTL 내 동일 groupResource 반복 미스가 그룹 재조회를 유발하지 않음.
    @Test
    void existingGroup_missingResource_negativeCachedPerResource() throws Exception {
      mockApiCallRepeatable("/apis/qa.example.io", """
          {
            "kind": "APIGroup",
            "name": "qa.example.io",
            "versions": [{"groupVersion": "qa.example.io/v1", "version": "v1"}]
          }
          """);
      mockApiCallRepeatable("/apis/qa.example.io/v1", """
          {
            "resources": [
              {"name": "widgets", "kind": "Widget", "namespaced": true}
            ]
          }
          """);

      assertThat(discovery.isExist("qa.example.io/gadgets")).isFalse();
      assertThat(discovery.isExist("qa.example.io/gadgets")).isFalse();
      // targeted refresh로 병합된 같은 그룹의 실제 리소스는 히트
      assertThat(discovery.isExist("qa.example.io/widgets")).isTrue();

      verifyBuildCallCount("/apis/qa.example.io", 1);
      verifyBuildCallCount("/apis/qa.example.io/v1", 1);
    }

    private void mockApiCallRepeatable(String path, String responseBody) throws Exception {
      Call call = mock(Call.class);
      // Response body는 1회성이므로 호출마다 새 Response 생성
      when(call.execute()).thenAnswer(invocation -> buildJsonResponse(path, 200, responseBody));
      when(mockApiClient.buildCall(
          any(), eq(path), any(), any(), any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(call);
    }

    private void mockApiCallNotFound(String path) throws Exception {
      Call call = mock(Call.class);
      when(call.execute()).thenAnswer(invocation -> buildJsonResponse(path, 404, """
          {"kind": "Status", "status": "Failure", "reason": "NotFound", "code": 404}
          """));
      when(mockApiClient.buildCall(
          any(), eq(path), any(), any(), any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(call);
    }

    private Response buildJsonResponse(String path, int code, String body) {
      return new Response.Builder()
          .request(new Request.Builder().url("https://localhost:6443" + path).build())
          .protocol(Protocol.HTTP_1_1)
          .code(code)
          .message(code == 200 ? "OK" : "Not Found")
          .body(ResponseBody.create(body, MediaType.get("application/json")))
          .build();
    }

    private void verifyBuildCallCount(String path, int expectedCount) throws Exception {
      verify(mockApiClient, times(expectedCount)).buildCall(
          any(), eq(path), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    private void verifyNoBuildCall() {
      try {
        verify(mockApiClient, never()).buildCall(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }
  }
}
