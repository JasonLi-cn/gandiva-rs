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

use crate::physical_plan::gandiva_filter_exec::GandivaFilterExec;
use crate::physical_plan::gandiva_projection_exec::GandivaProjectionExec;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::data_gen::create_random_batch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;
use tokio::runtime::Runtime;

pub async fn build_plan(state: &SessionState, sql: &str) -> Arc<dyn ExecutionPlan> {
    let logical_plan = state.create_logical_plan(sql).await.unwrap();
    state.create_physical_plan(&logical_plan).await.unwrap()
}

pub fn to_gandiva_projection_exec(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let gandiva_projection_exec: GandivaProjectionExec = plan.try_into().unwrap();
    Arc::new(gandiva_projection_exec)
}

pub fn to_gandiva_filter_exec(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let gandiva_filter_exec: GandivaFilterExec = plan.try_into().unwrap();
    Arc::new(gandiva_filter_exec)
}

pub async fn run_plan(state: &SessionState, plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    let task_ctx = Arc::new(TaskContext::from(state));
    collect(plan, task_ctx).await.unwrap()
}

pub fn create_ctx(sizes: &[usize], nullable: bool) -> SessionContext {
    let mut config = SessionConfig::new();
    config.options_mut().execution.coalesce_batches = false;

    let mut ctx = SessionContext::new_with_config(config);
    for size in sizes {
        register_mem_table(&mut ctx, *size, nullable);
    }
    ctx
}

pub fn register_mem_table(ctx: &mut SessionContext, size: usize, nullable: bool) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("t_boolean_1", DataType::Boolean, nullable),
        Field::new("t_boolean_2", DataType::Boolean, nullable),
        Field::new("t_boolean_3", DataType::Boolean, nullable),
        Field::new("t_int32_1", DataType::Int32, nullable),
        Field::new("t_int32_2", DataType::Int32, nullable),
        Field::new("t_int64_1", DataType::Int64, nullable),
        Field::new("t_int64_2", DataType::Int64, nullable),
        Field::new("t_int64_3", DataType::Int64, nullable),
        Field::new("t_float32_1", DataType::Float32, nullable),
        Field::new("t_utf8_1", DataType::Utf8, nullable),
        Field::new("t_utf8_2", DataType::Utf8, nullable),
        Field::new("t_utf8_3", DataType::Utf8, nullable),
    ]));
    let batch = create_random_batch(schema.clone(), size, 0.2, 0.8).unwrap();
    let tab = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let table_name = format!("tab{}", size);
    ctx.register_table(&table_name, tab).unwrap();
}

////// For Benches //////

pub fn build_plan_block(
    runtime: &Runtime,
    state: &SessionState,
    sql: &str,
) -> Arc<dyn ExecutionPlan> {
    runtime.block_on(async { build_plan(state, sql).await })
}

pub fn run_plan_block(
    runtime: &Runtime,
    state: &SessionState,
    plan: Arc<dyn ExecutionPlan>,
) -> Vec<RecordBatch> {
    runtime.block_on(async { run_plan(state, plan).await })
}
