// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{
    DFSchemaRef, DataFusionError, Diagnostic, Result, Span, not_impl_err, plan_err,
};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    Expr as SQLExpr, Ident, SelectItem, SetExpr, SetOperator, SetQuantifier, Spanned,
};

impl<S: ContextProvider> SqlToRel<'_, S> {
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    pub(super) fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let set_expr_span = Span::try_from_sqlparser_span(set_expr.span());
        match set_expr {
            SetExpr::Select(s) => self.select_to_plan(*s, None, planner_context),
            SetExpr::Values(v) => self.sql_values_to_plan(v, planner_context),
            SetExpr::SetOperation {
                op,
                left,
                right,
                set_quantifier,
            } => {
                let left_span = Span::try_from_sqlparser_span(left.span());
                let right_span = Span::try_from_sqlparser_span(right.span());
                let left_plan = self.set_expr_to_plan(*left, planner_context);

                // For non-*ByName operations, add missing aliases to right side using left schema's
                // column names. This allows queries like
                // `SELECT 1 c1, 0 c2, 0 c3 UNION ALL SELECT 2, 0, 0`
                // where the right side has duplicate literal values.
                // We only do this if the left side succeeded.
                let right = if let Ok(plan) = &left_plan
                    && plan.schema().fields().len() > 1
                    && matches!(
                        set_quantifier,
                        SetQuantifier::All
                            | SetQuantifier::Distinct
                            | SetQuantifier::None
                    ) {
                    alias_set_expr(*right, plan.schema())
                } else {
                    *right
                };

                let right_plan = self.set_expr_to_plan(right, planner_context);

                // Handle errors from both sides, collecting them if both failed
                let (left_plan, right_plan) = match (left_plan, right_plan) {
                    (Ok(left_plan), Ok(right_plan)) => (left_plan, right_plan),
                    (Err(left_err), Err(right_err)) => {
                        return Err(DataFusionError::Collection(vec![
                            left_err, right_err,
                        ]));
                    }
                    (Err(err), _) | (_, Err(err)) => {
                        return Err(err);
                    }
                };
                if !(set_quantifier == SetQuantifier::ByName
                    || set_quantifier == SetQuantifier::AllByName)
                {
                    self.validate_set_expr_num_of_columns(
                        op,
                        left_span,
                        right_span,
                        &left_plan,
                        &right_plan,
                        set_expr_span,
                    )?;
                }
                self.set_operation_to_plan(op, left_plan, right_plan, set_quantifier)
            }
            SetExpr::Query(q) => self.query_to_plan(*q, planner_context),
            _ => not_impl_err!("Query {set_expr} not implemented yet"),
        }
    }

    pub(super) fn is_union_all(set_quantifier: SetQuantifier) -> Result<bool> {
        match set_quantifier {
            SetQuantifier::All | SetQuantifier::AllByName => Ok(true),
            SetQuantifier::Distinct
            | SetQuantifier::ByName
            | SetQuantifier::DistinctByName
            | SetQuantifier::None => Ok(false),
        }
    }

    fn validate_set_expr_num_of_columns(
        &self,
        op: SetOperator,
        left_span: Option<Span>,
        right_span: Option<Span>,
        left_plan: &LogicalPlan,
        right_plan: &LogicalPlan,
        set_expr_span: Option<Span>,
    ) -> Result<()> {
        if left_plan.schema().fields().len() == right_plan.schema().fields().len() {
            return Ok(());
        }
        let diagnostic = Diagnostic::new_error(
            format!("{op} queries have different number of columns"),
            set_expr_span,
        )
        .with_note(
            format!("this side has {} fields", left_plan.schema().fields().len()),
            left_span,
        )
        .with_note(
            format!(
                "this side has {} fields",
                right_plan.schema().fields().len()
            ),
            right_span,
        );
        plan_err!("{} queries have different number of columns", op; diagnostic =diagnostic)
    }

    pub(super) fn set_operation_to_plan(
        &self,
        op: SetOperator,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        set_quantifier: SetQuantifier,
    ) -> Result<LogicalPlan> {
        match (op, set_quantifier) {
            (SetOperator::Union, SetQuantifier::All) => {
                LogicalPlanBuilder::from(left_plan)
                    .union(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::AllByName) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_by_name(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_distinct(right_plan)?
                    .build()
            }
            (
                SetOperator::Union,
                SetQuantifier::ByName | SetQuantifier::DistinctByName,
            ) => LogicalPlanBuilder::from(left_plan)
                .union_by_name_distinct(right_plan)?
                .build(),
            (SetOperator::Intersect, SetQuantifier::All) => {
                LogicalPlanBuilder::intersect(left_plan, right_plan, true)
            }
            (SetOperator::Intersect, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::intersect(left_plan, right_plan, false)
            }
            (SetOperator::Except, SetQuantifier::All) => {
                LogicalPlanBuilder::except(left_plan, right_plan, true)
            }
            (SetOperator::Except, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::except(left_plan, right_plan, false)
            }
            (op, quantifier) => {
                not_impl_err!("{op} {quantifier} not implemented")
            }
        }
    }
}

// Adds aliases to SELECT items in a SetExpr using the provided schema.
// This ensures that unnamed expressions on the right side of a UNION/INTERSECT/EXCEPT
// get aliased with the column names from the left side, allowing queries like
// `SELECT 1 AS a, 0 AS b, 0 AS c UNION ALL SELECT 2, 0, 0` to work correctly.
fn alias_set_expr(set_expr: SetExpr, schema: &DFSchemaRef) -> SetExpr {
    match set_expr {
        SetExpr::Select(mut select) => {
            alias_select_items(&mut select.projection, schema);
            SetExpr::Select(select)
        }
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => {
            // For nested set operations, only alias the leftmost branch
            // since that's what determines the output column names
            SetExpr::SetOperation {
                op,
                left: Box::new(alias_set_expr(*left, schema)),
                right,
                set_quantifier,
            }
        }
        SetExpr::Query(mut query) => {
            // Handle parenthesized queries like (SELECT ... UNION ALL SELECT ...)
            query.body = Box::new(alias_set_expr(*query.body, schema));
            SetExpr::Query(query)
        }
        // For other cases (Values, etc.), return as-is
        other => other,
    }
}

// Adds aliases to literal value expressions where missing, based on the input schema, as these are
// the ones that can cause duplicate name issues (e.g. `SELECT 0, 0` has two columns named `Int64(0)`).
// Other expressions typically have unique names.
fn alias_select_items(items: &mut [SelectItem], schema: &DFSchemaRef) {
    let mut col_idx = 0;
    for item in items.iter_mut() {
        match item {
            SelectItem::UnnamedExpr(expr) if is_literal_value(expr) => {
                if let Some(field) = schema.fields().get(col_idx) {
                    *item = SelectItem::ExprWithAlias {
                        expr: expr.clone(),
                        alias: Ident::new(field.name()),
                    };
                }
                col_idx += 1;
            }
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {
                col_idx += 1;
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                // Wildcards expand to multiple columns - skip position tracking
            }
        }
    }
}

/// Returns true if the expression is a literal value that could cause duplicate names.
fn is_literal_value(expr: &SQLExpr) -> bool {
    matches!(
        expr,
        SQLExpr::Value(_)
            | SQLExpr::UnaryOp { .. }
            | SQLExpr::TypedString { .. }
            | SQLExpr::Interval(_)
    )
}
