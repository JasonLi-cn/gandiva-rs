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
    build_plan, create_ctx, run_plan, to_gandiva_filter_exec,
};

#[tokio::test]
async fn test_filter() {
    let sizes = vec![16];
    let ctx = create_ctx(&sizes, true);
    let state = ctx.state();

    let sqls = [
        "select * from tab16 where t_int32_1 > 1",
        "select * from tab16 where length(t_utf8_1) > 2",
    ];

    for sql in sqls {
        let plan = build_plan(&state, sql).await;
        let vector_result = run_plan(&state, plan.clone()).await;
        arrow::util::pretty::print_batches(&vector_result).unwrap();

        let gandiva_plan = to_gandiva_filter_exec(plan);
        let gandiva_result = run_plan(&state, gandiva_plan).await;
        arrow::util::pretty::print_batches(&gandiva_result).unwrap();

        assert_eq!(vector_result, gandiva_result);
    }
}
