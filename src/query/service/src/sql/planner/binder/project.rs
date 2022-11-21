// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use common_ast::ast::Indirection;
use common_ast::ast::SelectTarget;
use common_exception::ErrorCode;
use common_exception::Result;

use super::bind_context::NameResolutionResult;
use crate::sql::binder::select::SelectItem;
use crate::sql::binder::select::SelectList;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::binder::scalar::ScalarBinder;
use crate::sql::planner::binder::BindContext;
use crate::sql::planner::binder::Binder;
use crate::sql::planner::binder::ColumnBinding;
use crate::sql::planner::semantic::normalize_identifier;
use crate::sql::planner::semantic::GroupingChecker;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::Project;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::plans::ScalarItem;
use crate::sql::plans::SubqueryExpr;
use crate::sql::plans::SubqueryType;
use crate::sql::IndexType;

impl<'a> Binder {
    pub(super) fn analyze_projection(
        &mut self,
        select_list: &SelectList<'a>,
    ) -> Result<(HashMap<IndexType, ScalarItem>, Vec<ColumnBinding>)> {
        let mut columns = Vec::with_capacity(select_list.items.len());
        let mut scalars = HashMap::new();
        for item in select_list.items.iter() {
            let column_binding = if let Scalar::BoundColumnRef(ref column_ref) = item.scalar {
                let mut column_binding = column_ref.column.clone();
                // We should apply alias for the ColumnBinding, since it comes from table
                column_binding.column_name = item.alias.clone();
                column_binding
            } else {
                self.create_column_binding(None, None, item.alias.clone(), item.scalar.data_type())
            };
            let scalar = if let Scalar::SubqueryExpr(SubqueryExpr {
                typ,
                subquery,
                child_expr,
                compare_op,
                data_type,
                allow_multi_rows,
                outer_columns,
                ..
            }) = item.scalar.clone()
            {
                if typ == SubqueryType::Any || typ == SubqueryType::Exists {
                    Scalar::SubqueryExpr(SubqueryExpr {
                        typ,
                        subquery,
                        child_expr,
                        compare_op,
                        index: Some(column_binding.index),
                        data_type,
                        allow_multi_rows,
                        outer_columns,
                    })
                } else {
                    item.scalar.clone()
                }
            } else {
                item.scalar.clone()
            };
            scalars.insert(column_binding.index, ScalarItem {
                scalar,
                index: column_binding.index,
            });
            columns.push(column_binding);
        }

        Ok((scalars, columns))
    }

    pub(super) fn bind_projection(
        &mut self,
        bind_context: &mut BindContext,
        columns: &[ColumnBinding],
        scalars: &HashMap<IndexType, ScalarItem>,
        child: SExpr,
    ) -> Result<SExpr> {
        let scalars = scalars
            .iter()
            .map(|(_, item)| {
                if bind_context.in_grouping {
                    let mut grouping_checker = GroupingChecker::new(bind_context);
                    let scalar = grouping_checker.resolve(&item.scalar, None)?;
                    Ok(ScalarItem {
                        scalar,
                        index: item.index,
                    })
                } else {
                    Ok(item.clone())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let eval_scalar = EvalScalar { items: scalars };

        let mut new_expr = SExpr::create_unary(eval_scalar.into(), child);

        let mut col_set = ColumnSet::new();
        for item in columns.iter() {
            col_set.insert(item.index);
        }

        // Set output columns
        bind_context.columns = columns.to_vec();

        let project_plan = Project { columns: col_set };

        new_expr = SExpr::create_unary(project_plan.into(), new_expr);
        Ok(new_expr)
    }

    /// Normalize select list into a BindContext.
    /// There are three kinds of select target:
    ///
    ///   * Qualified name, e.g. `SELECT t.a FROM t`
    ///   * Qualified name with wildcard, e.g. `SELECT t.* FROM t, t1`
    ///   * Scalar expression or aggregate expression, e.g. `SELECT COUNT(*)+1 AS count FROM t`
    ///
    /// For qualified names, we just resolve it with the input `BindContext`. If successful, we
    /// will get a `ColumnBinding` and the `expr` field is left `None`.
    ///
    /// The qualified names with wildcard will be expanded into qualified names for resolution.
    /// For example, `SELECT * FROM t` may be expanded into `SELECT t.a, t.b FROM t`.
    ///
    /// For scalar expressions and aggregate expressions, we will register new columns for
    /// them in `Metadata`. And notice that, the semantic of aggregate expressions won't be checked
    /// in this function.
    pub(super) async fn normalize_select_list(
        &mut self,
        input_context: &BindContext,
        select_list: &'a [SelectTarget<'a>],
    ) -> Result<SelectList<'a>> {
        let mut output = SelectList::<'a>::default();
        for select_target in select_list {
            match select_target {
                SelectTarget::QualifiedName(names) => {
                    // Handle qualified name as select target
                    if names.len() == 1 {
                        let indirection = &names[0];
                        match indirection {
                            Indirection::Identifier(ident) => {
                                let result = input_context.resolve_name(
                                    None,
                                    None,
                                    &normalize_identifier(ident, &self.name_resolution_ctx).name,
                                    &ident.span,
                                    &[],
                                )?;
                                output.items.push(SelectItem {
                                    select_target,
                                    scalar: match result {
                                        NameResolutionResult::Column(column) => {
                                            BoundColumnRef { column }.into()
                                        }
                                        NameResolutionResult::Alias { scalar, .. } => scalar,
                                    },

                                    alias: ident.name.clone(),
                                });
                            }
                            Indirection::Star => {
                                // Expands wildcard star, for example we have a table `t(a INT, b INT)`:
                                // The query `SELECT * FROM t` will be expanded into `SELECT t.a, t.b FROM t`
                                for column_binding in input_context.all_column_bindings() {
                                    if !column_binding.visible_in_unqualified_wildcard {
                                        continue;
                                    }
                                    output.items.push(SelectItem {
                                        select_target,
                                        scalar: BoundColumnRef {
                                            column: column_binding.clone(),
                                        }
                                        .into(),
                                        alias: column_binding.column_name.clone(),
                                    });
                                }
                            }
                        }
                    } else {
                        // TODO: Support indirection like `a.b`, `a.*`
                        return Err(ErrorCode::SemanticError("Unsupported indirection type"));
                    }
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let mut scalar_binder = ScalarBinder::new(
                        input_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        &[],
                    );
                    let (bound_expr, _) = scalar_binder.bind(expr).await?;

                    // If alias is not specified, we will generate a name for the scalar expression.
                    let expr_name = match alias {
                        Some(alias) => normalize_identifier(alias, &self.name_resolution_ctx).name,
                        None => format!("{:#}", expr),
                    };

                    output.items.push(SelectItem {
                        select_target,
                        scalar: bound_expr,
                        alias: expr_name,
                    });
                }
            }
        }
        if output.items.is_empty() {
            Err(ErrorCode::SemanticError("SELECT with no columns"))
        } else {
            Ok(output)
        }
    }
}
