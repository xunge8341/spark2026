use criterion::{Criterion, criterion_group, criterion_main};
use spark_core::configuration::{
    ConfigKey, ConfigMetadata, ConfigScope, ConfigValue, ConfigurationBuilder, ConfigurationLayer,
    ProfileDescriptor, ProfileId, ProfileLayering, SourceMetadata,
};

/// Benchmark: 构建带有单一静态层的配置。
///
/// *Why*：验证 `ConfigurationBuilder` 的装配开销，确保在默认路径上构建动作轻量。
/// *How*：循环 100 次构建一个只包含静态 Layer 的配置句柄。
/// *What*：基准输出关注每次构建耗时，帮助评估初始化路径。
fn bench_build_static_layer(c: &mut Criterion) {
    c.bench_function("configuration_build_static_layer", |b| {
        b.iter(|| {
            let mut builder = ConfigurationBuilder::new().with_profile(ProfileDescriptor::new(
                ProfileId::new("bench"),
                vec![],
                ProfileLayering::BaseFirst,
                "bench profile",
            ));

            struct StaticSource;
            impl spark_core::configuration::ConfigurationSource for StaticSource {
                type Stream<'a>
                    = spark_core::configuration::NoopConfigStream
                where
                    Self: 'a;

                fn load(
                    &self,
                    _profile: &ProfileId,
                ) -> spark_core::Result<
                    Vec<ConfigurationLayer>,
                    spark_core::configuration::ConfigurationError,
                > {
                    Ok(vec![ConfigurationLayer {
                        metadata: SourceMetadata::new("bench", 0, None),
                        entries: vec![(
                            ConfigKey::new("bench", "flag", ConfigScope::Global, "bench flag"),
                            ConfigValue::Boolean(true, ConfigMetadata::default()),
                        )],
                    }])
                }

                fn watch<'a>(
                    &'a self,
                    _profile: &ProfileId,
                ) -> spark_core::Result<
                    Self::Stream<'a>,
                    spark_core::configuration::ConfigurationError,
                > {
                    Ok(spark_core::configuration::NoopConfigStream::new())
                }
            }

            builder
                .register_source(Box::new(StaticSource))
                .expect("register source");

            let outcome = builder.build().expect("build configuration");
            criterion::black_box(outcome.report.snapshot().to_json());
        });
    });
}

criterion_group!(configuration_benches, bench_build_static_layer);
criterion_main!(configuration_benches);
