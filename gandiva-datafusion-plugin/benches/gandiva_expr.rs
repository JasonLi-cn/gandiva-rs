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
    build_plan_block, create_ctx, run_plan_block, to_gandiva_projection_exec,
};

fn criterion_benchmark(c: &mut Criterion) {
    let sizes = vec![1024, 8192];
    let ctx = create_ctx(&sizes, true);
    let state = ctx.state();
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();

    let sqls = [
        // binary: control
        ("select t_boolean_1 and true from tab", "Andx0"),
        ("select t_boolean_1 or false from tab", "Orx0"),
        ("select t_boolean_1 and t_boolean_2 from tab", "Andx1"),
        ("select t_boolean_1 or t_boolean_2 from tab", "Orx1"),
        (
            "select t_boolean_1 and t_boolean_2 and t_boolean_3 from tab",
            "Andx2",
        ),
        (
            "select t_boolean_1 or t_boolean_2 or t_boolean_3 from tab",
            "Orx2",
        ),
        // binary: math
        ("select t_int64_1 + 1024 from tab", "Plusx0"),
        ("select t_int64_1 - 1024 from tab", "Minusx0"),
        ("select t_int64_1 * 1024 from tab", "Multiplyx0"),
        ("select t_int64_1 / 1024 from tab", "Dividex0"),
        ("select t_int64_1 % 1024 from tab", "Modulox0"),
        ("select t_int64_1 + t_int64_2 from tab", "Plusx1"),
        ("select t_int64_1 - t_int64_2 from tab", "Minusx1"),
        ("select t_int64_1 * t_int64_2 from tab", "Multiplyx1"),
        ("select t_int64_1 / t_int64_2 from tab", "Dividex1"),
        ("select t_int64_1 % t_int64_2 from tab", "Modulox1"),
        (
            "select t_int64_1 + t_int64_2 + t_int64_3 from tab",
            "Plusx2",
        ),
        (
            "select t_int64_1 - t_int64_2 - t_int64_3 from tab",
            "Minusx2",
        ),
        (
            "select t_int64_1 * t_int64_2 * t_int64_3 from tab",
            "Multiplyx2",
        ),
        (
            "select t_int64_1 / t_int64_2 / t_int64_3 from tab",
            "Dividex2",
        ),
        (
            "select t_int64_1 % t_int64_2 % t_int64_3 from tab",
            "Modulox2",
        ),
        // binary: compare
        ("select t_int64_1 = 1024 from tab", "Eqx0"),
        ("select t_int64_1 < 1024 from tab", "Ltx0"),
        ("select t_int64_1 <= 1024 from tab", "LtEqx0"),
        ("select t_int64_1 > 1024 from tab", "Gtx0"),
        ("select t_int64_1 >= 1024 from tab", "GtEqx0"),
        ("select t_int64_1 = t_int64_2 from tab", "Eqx1"),
        ("select t_int64_1 < t_int64_2 from tab", "Ltx1"),
        ("select t_int64_1 <= t_int64_2 from tab", "LtEqx1"),
        ("select t_int64_1 > t_int64_2 from tab", "Gtx1"),
        ("select t_int64_1 >= t_int64_2 from tab", "GtEqx1"),
        // in list
        // ("select t_int64_1 in (0) from tab", "InListx1"),
        // ("select t_int64_1 in (0, 1) from tab", "InListx2"),
        // ("select t_int64_1 in (0, 1, 2) from tab", "InListx3"),
        // is not null
        ("select t_int64_1 is not null from tab", "IsNotNullx0"),
        // is null
        ("select t_int64_1 is null from tab", "IsNullx0"),
        // like
        ("select t_utf8_1 like 'C%' from tab", "Likex0"),
        ("select t_utf8_1 like '%C' from tab", "Likex1"),
        ("select t_utf8_1 like '%C%' from tab", "Likex2"),
        // functions
        ("select concat(t_utf8_1, 'ABCD') from tab", "Concatx0"),
        ("select concat(t_utf8_1, t_utf8_2) from tab", "Concatx1"),
        (
            "select concat(t_utf8_1, t_utf8_2, t_utf8_3) from tab",
            "Concatx2",
        ),
        ("select length(t_utf8_1) from tab", "Lengthx0"),
        ("select lower(t_utf8_1) from tab", "Lowerx0"),
        ("select replace(t_utf8_1, 'A', 'AA') from tab", "Replacex0"),
        (
            "select length(lower(t_utf8_1)) from tab",
            "ComplexLengthLowerx0",
        ),
        (
            "select replace(lower(t_utf8_1), 'a', 'aa') from tab",
            "ComplexReplaceLowerx0",
        ),
    ];

    for (sql, group_name) in sqls {
        let mut group = c.benchmark_group(group_name);
        for size in &sizes {
            let sql = sql.replace("tab", &format!("tab{}", size)); // specify table with size
            let plan = build_plan_block(&runtime, &state, &sql);

            group.bench_function(BenchmarkId::new("Vector", size), |b| {
                b.iter(|| criterion::black_box(run_plan_block(&runtime, &state, plan.clone())))
            });
            let vector_result = run_plan_block(&runtime, &state, plan.clone());

            let gandiva_plan = to_gandiva_projection_exec(plan);
            group.bench_function(BenchmarkId::new("Gandiva", size), |b| {
                b.iter(|| {
                    criterion::black_box(run_plan_block(&runtime, &state, gandiva_plan.clone()))
                })
            });
            let gandiva_result = run_plan_block(&runtime, &state, gandiva_plan.clone());

            assert_eq!(vector_result, gandiva_result);
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
