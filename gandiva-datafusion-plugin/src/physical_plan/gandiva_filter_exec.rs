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

use crate::physical_expr::condition::to_gandiva_filter;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::{Stream, StreamExt};
use gandiva_rs_bindings::evaluator::filter::FilterRef;
use log::trace;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone)]
pub struct GandivaFilterExec {
    inner: Arc<FilterExec>,
    gandiva_filter: FilterRef,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for GandivaFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl GandivaFilterExec {
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let filter_exec = FilterExec::try_new(predicate, input)?;
        filter_exec.try_into()
    }

    pub fn inner(&self) -> &Arc<FilterExec> {
        &self.inner
    }
}

impl TryFrom<FilterExec> for GandivaFilterExec {
    type Error = DataFusionError;

    fn try_from(filter: FilterExec) -> std::result::Result<Self, Self::Error> {
        let predicate = filter.predicate();
        let input_schema = filter.input().schema();
        let gandiva_filter = to_gandiva_filter(predicate, &input_schema)?;
        Ok(Self {
            inner: Arc::new(filter),
            gandiva_filter,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl TryFrom<&FilterExec> for GandivaFilterExec {
    type Error = DataFusionError;
    fn try_from(filter: &FilterExec) -> std::result::Result<Self, Self::Error> {
        let predicate = filter.predicate().clone();
        let input = filter.input().clone();
        let input_schema = filter.input().schema();

        let gandiva_filter = to_gandiva_filter(&predicate, &input_schema)?;
        let inner = Arc::new(FilterExec::try_new(predicate, input)?);

        Ok(Self {
            inner,
            gandiva_filter,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl TryFrom<Arc<dyn ExecutionPlan>> for GandivaFilterExec {
    type Error = DataFusionError;
    fn try_from(plan: Arc<dyn ExecutionPlan>) -> std::result::Result<Self, Self::Error> {
        match plan.as_any().downcast_ref::<FilterExec>() {
            Some(filter) => filter.try_into(),
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

impl ExecutionPlan for GandivaFilterExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        // The filter operator does not make any changes to the schema of its input
        self.inner.schema()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        self.inner.unbounded_output(children)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.inner.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // tell optimizer this operator doesn't reorder its input
        self.inner.maintains_input_order()
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter_exec =
            FilterExec::try_new(self.inner.predicate().clone(), children.swap_remove(0)).and_then(
                |e| {
                    let selectivity = e.default_selectivity();
                    e.with_default_selectivity(selectivity)
                },
            )?;
        let gandiva_filter: GandivaFilterExec = filter_exec.try_into()?;
        Ok(Arc::new(gandiva_filter))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start FilterExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(GandivaFilterExecStream {
            schema: self.inner.input().schema(),
            gandiva_filter: self.gandiva_filter.clone(),
            input: self.inner.input().execute(partition, context)?,
            baseline_metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The output statistics of a filtering operation can be estimated if the
    /// predicate's selectivity value can be determined for the incoming data.
    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }
}

/// The FilterExec streams wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct GandivaFilterExecStream {
    /// Output schema, which is the same as the input schema for this operator
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    gandiva_filter: FilterRef,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// runtime metrics recording
    baseline_metrics: BaselineMetrics,
}

impl GandivaFilterExecStream {
    pub(crate) fn batch_filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = batch.columns();
        let sel_vec = self
            .gandiva_filter
            .evaluate0(columns)
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        let filter_array = sel_vec.to_boolean_array();
        Ok(filter_record_batch(batch, &filter_array)?)
    }
}

impl Stream for GandivaFilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(value) => match value {
                    Some(Ok(batch)) => {
                        let timer = self.baseline_metrics.elapsed_compute().timer();
                        let filtered_batch = self.batch_filter(&batch)?;
                        // skip entirely filtered batches
                        if filtered_batch.num_rows() == 0 {
                            continue;
                        }
                        timer.done();
                        poll = Poll::Ready(Some(Ok(filtered_batch)));
                        break;
                    }
                    _ => {
                        poll = Poll::Ready(value);
                        break;
                    }
                },
                Poll::Pending => {
                    poll = Poll::Pending;
                    break;
                }
            }
        }
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for GandivaFilterExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
