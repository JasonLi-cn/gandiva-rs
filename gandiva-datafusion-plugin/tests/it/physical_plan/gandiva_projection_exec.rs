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

use gandiva_datafusion_plugin::tests::utils::{
    build_plan, create_ctx, run_plan, to_gandiva_projection_exec,
};

#[tokio::test]
async fn test_projection() {
    let sizes = vec![16];
    let ctx = create_ctx(&sizes, true);
    let state = ctx.state();

    let sqls = [
        "select concat(t_utf8_1, 'OK') from tab16",
        "select t_boolean_1 or false from tab16",
        "select t_int64_1 in (0, 1, 2) from tab16",
        "select t_int64_1 is not null from tab16",
        "select t_int64_1 is null from tab16",
        "select t_utf8_1 like 'C%' from tab16",
        "select concat(t_utf8_1, 'ABCD') from tab16",
        "select concat(t_utf8_1, t_utf8_2) from tab16",
        "select concat(t_utf8_1, t_utf8_2, t_utf8_3) from tab16",
        "select length(t_utf8_1) from tab16",
        "select lower(t_utf8_1) from tab16",
        "select replace(t_utf8_1, 'A', 'AA') from tab16",
        "select replace(lower(t_utf8_1), 'a', 'aa') from tab16",
    ];

    for sql in sqls {
        let plan = build_plan(&state, sql).await;

        let display = plan.clone();
        let s = datafusion::physical_plan::display::DisplayableExecutionPlan::new(&*display)
            .indent(false);
        println!("{}", s);

        let vector_result = run_plan(&state, plan.clone()).await;
        arrow::util::pretty::print_batches(&vector_result).unwrap();

        let gandiva_plan = to_gandiva_projection_exec(plan);
        let gandiva_result = run_plan(&state, gandiva_plan).await;
        arrow::util::pretty::print_batches(&gandiva_result).unwrap();

        assert_eq!(vector_result, gandiva_result);
    }
}
