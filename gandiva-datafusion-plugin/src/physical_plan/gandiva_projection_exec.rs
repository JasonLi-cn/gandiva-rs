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

use crate::physical_expr::expr::to_gandiva_projector;
use arrow::array::RecordBatch;
use arrow::record_batch::RecordBatchOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use gandiva_rs_bindings::evaluator::projector::ProjectorRef;
use log::trace;
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{Stream, StreamExt};

#[derive(Debug, Clone)]
pub struct GandivaProjectionExec {
    inner: Arc<ProjectionExec>,
    projector: ProjectorRef,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for GandivaProjectionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl GandivaProjectionExec {
    /// Create a projection on an input
    pub fn try_new(
        expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let inner = ProjectionExec::try_new(expr, input)?;
        inner.try_into()
    }

    pub fn inner(&self) -> &Arc<ProjectionExec> {
        &self.inner
    }
}

impl TryFrom<ProjectionExec> for GandivaProjectionExec {
    type Error = DataFusionError;

    fn try_from(projection: ProjectionExec) -> std::result::Result<Self, Self::Error> {
        let exprs = projection.expr();
        let input_schema = projection.input().schema();
        let output_schema = projection.schema();
        let projector = to_gandiva_projector(exprs, &input_schema, &output_schema)?;
        Ok(Self {
            inner: Arc::new(projection),
            projector,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl TryFrom<&ProjectionExec> for GandivaProjectionExec {
    type Error = DataFusionError;
    fn try_from(projection: &ProjectionExec) -> std::result::Result<Self, Self::Error> {
        projection.clone().try_into()
    }
}

impl TryFrom<Arc<dyn ExecutionPlan>> for GandivaProjectionExec {
    type Error = DataFusionError;
    fn try_from(plan: Arc<dyn ExecutionPlan>) -> std::result::Result<Self, Self::Error> {
        match plan.as_any().downcast_ref::<ProjectionExec>() {
            Some(projection) => projection.try_into(),
            None => {
                let dis_plan = DisplayableExecutionPlan::with_metrics(&*plan).indent(false);
                Err(DataFusionError::Plan(format!(
                    "Can't convert {} to GandivaProjectionExec",
                    dis_plan
                )))
            }
        }
    }
}

impl ExecutionPlan for GandivaProjectionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        self.inner.unbounded_output(children)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.inner.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.inner.equivalence_properties()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projection_exec =
            ProjectionExec::try_new(self.inner.expr().to_vec(), children.swap_remove(0))?;
        let gandiva_projection_exec: GandivaProjectionExec = projection_exec.try_into()?;
        Ok(Arc::new(gandiva_projection_exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        trace!("Start GandivaProjectionExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        Ok(Box::pin(GandivaProjectionStream {
            schema: self.inner.schema().clone(),
            projector: self.projector.clone(),
            input: self.inner.input().execute(partition, context)?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }
}

struct GandivaProjectionStream {
    schema: SchemaRef,
    projector: ProjectorRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl GandivaProjectionStream {
    fn batch_project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        let columns = batch.columns();
        if columns.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            return RecordBatch::try_new_with_options(self.schema.clone(), vec![], &options)
                .map_err(Into::into);
        }
        self.projector
            .evaluate1(columns)
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))
    }
}

impl Stream for GandivaProjectionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.batch_project(&batch)),
            other => other,
        });

        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for GandivaProjectionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
