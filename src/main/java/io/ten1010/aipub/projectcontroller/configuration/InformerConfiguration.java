package io.ten1010.aipub.projectcontroller.configuration;

import io.kubernetes.client.informer.SharedInformerFactory;
import io.ten1010.aipub.projectcontroller.domain.k8s.K8sApiProvider;
import io.ten1010.aipub.projectcontroller.informer.InformerRegistrar;
import io.ten1010.aipub.projectcontroller.informer.SharedInformerFactoryProvider;
import io.ten1010.aipub.projectcontroller.informer.dynamic.DynamicCrInformerManager;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InformerConfiguration {

  @Bean
  public SharedInformerFactory sharedInformerFactory(K8sApiProvider k8sApiProvider,
      List<InformerRegistrar> registrars) {
    return new SharedInformerFactoryProvider(k8sApiProvider,
        registrars).createSharedInformerFactory();
  }

  @Bean
  public DynamicCrInformerManager dynamicCrInformerManager(
      K8sApiProvider k8sApiProvider, SharedInformerFactory sharedInformerFactory) {
    return new DynamicCrInformerManager(k8sApiProvider.getApiClient(), sharedInformerFactory);
  }

}
