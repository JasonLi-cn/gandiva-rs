/*
 * Copyright 2024 JasonLi-cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[macro_use]
extern crate criterion;

use criterion::{BenchmarkId, Criterion};
use gandiva_datafusion_plugin::tests::utils::{
    build_plan_block, create_ctx, run_plan_block, to_gandiva_filter_exec,
};

fn criterion_benchmark(c: &mut Criterion) {
    let sizes = vec![1024, 8192];
    let ctx = create_ctx(&sizes, true);
    let state = ctx.state();
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();

    let sqls = [
        "select * from tab where t_int32_1 > 1",
        "select * from tab where t_int32_1 > 1 and t_int64_1 < 10000",
        "select * from tab where t_int32_1 > 1 or t_int64_1 < 10000",
        "select * from tab where length(t_utf8_1) > 2",
        "select * from tab where t_utf8_1 like '%CA%'",
    ];

    for sql in sqls {
        let mut group = c.benchmark_group(sql);
        for size in &sizes {
            let sql = sql.replace("tab", &format!("tab{}", size)); // specify table with size
            let plan = build_plan_block(&runtime, &state, &sql);

            group.bench_function("Vector", |b| {
                b.iter(|| criterion::black_box(run_plan_block(&runtime, &state, plan.clone())))
            });
            let vector_result = run_plan_block(&runtime, &state, plan.clone());

            let gandiva_plan = to_gandiva_filter_exec(plan);
            group.bench_function("Gandiva", |b| {
                b.iter(|| {
                    criterion::black_box(run_plan_block(&runtime, &state, gandiva_plan.clone()))
                })
            });
            let gandiva_result = run_plan_block(&runtime, &state, gandiva_plan.clone());

            assert_eq!(vector_result, gandiva_result);
        }
        group.finish();
    }

    // for sql in sqls {
    //     for size in &sizes {
    //         let sql = sql.replace("tab", &format!("tab{}", size)); // specify table with size
    //         let plan = build_plan_block(&runtime, &state, &sql);
    //         c.bench_function(&format!("Vector: {}", sql), |b| {
    //             b.iter(|| criterion::black_box(run_plan_block(&runtime, &state, plan.clone())))
    //         });
    //         let vector_result = run_plan_block(&runtime, &state, plan.clone());
    //
    //         let gandiva_plan = to_gandiva_filter_exec(plan);
    //         c.bench_function(&format!("Gandiva: {}", sql), |b| {
    //             b.iter(|| {
    //                 criterion::black_box(run_plan_block(&runtime, &state, gandiva_plan.clone()))
    //             })
    //         });
    //         let gandiva_result = run_plan_block(&runtime, &state, gandiva_plan.clone());
    //
    //         assert_eq!(vector_result, gandiva_result);
    //     }
    // }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
