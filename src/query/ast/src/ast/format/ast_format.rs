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

use std::fmt::Display;

use common_datavalues::IntervalKind;
use common_exception::Result;
use common_meta_types::PrincipalIdentity;
use common_meta_types::UserIdentity;

use crate::ast::*;
use crate::parser::token::Token;
use crate::visitors::Visitor;

pub fn format_statement(stmt: Statement) -> Result<String> {
    let mut visitor = AstFormatVisitor::new();
    visitor.visit_statement(&stmt);
    let format_ctx = visitor.children.pop().unwrap();
    format_ctx.format_pretty()
}

#[derive(Clone)]
pub struct AstFormatContext {
    name: String,
    children_num: usize,
    alias: Option<String>,
}

impl AstFormatContext {
    pub fn new(name: String) -> Self {
        Self {
            name,
            children_num: 0,
            alias: None,
        }
    }

    pub fn with_children(name: String, children_num: usize) -> Self {
        Self {
            name,
            children_num,
            alias: None,
        }
    }

    pub fn with_children_alias(name: String, children_num: usize, alias: Option<String>) -> Self {
        Self {
            name,
            children_num,
            alias,
        }
    }
}

impl Display for AstFormatContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.alias {
            Some(alias) => {
                if self.children_num > 0 {
                    write!(
                        f,
                        "{} (alias {}) (children {})",
                        self.name, alias, self.children_num
                    )
                } else {
                    write!(f, "{} (alias {})", self.name, alias)
                }
            }
            None => {
                if self.children_num > 0 {
                    write!(f, "{} (children {})", self.name, self.children_num)
                } else {
                    write!(f, "{}", self.name)
                }
            }
        }
    }
}

pub struct AstFormatVisitor {
    children: Vec<FormatTreeNode<AstFormatContext>>,
}

impl AstFormatVisitor {
    pub fn new() -> Self {
        Self { children: vec![] }
    }
}

impl<'ast> Visitor<'ast> for AstFormatVisitor {
    fn visit_identifier(&mut self, ident: &'ast Identifier<'ast>) {
        let format_ctx = AstFormatContext::new(format!("Identifier {ident}"));
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_database_ref(
        &mut self,
        catalog: &'ast Option<Identifier<'ast>>,
        database: &'ast Identifier<'ast>,
    ) {
        let mut name = String::new();
        name.push_str("DatabaseIdentifier ");
        if let Some(catalog) = catalog {
            name.push_str(&catalog.to_string());
            name.push('.');
        }
        name.push_str(&database.to_string());
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_table_ref(
        &mut self,
        catalog: &'ast Option<Identifier<'ast>>,
        database: &'ast Option<Identifier<'ast>>,
        table: &'ast Identifier<'ast>,
    ) {
        let mut name = String::new();
        name.push_str("TableIdentifier ");
        if let Some(catalog) = catalog {
            name.push_str(&catalog.to_string());
            name.push('.');
        }
        if let Some(database) = database {
            name.push_str(&database.to_string());
            name.push('.');
        }
        name.push_str(&table.to_string());
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_column_ref(
        &mut self,
        _span: &'ast [Token<'ast>],
        database: &'ast Option<Identifier<'ast>>,
        table: &'ast Option<Identifier<'ast>>,
        column: &'ast Identifier<'ast>,
    ) {
        let mut name = String::new();
        name.push_str("ColumnIdentifier ");
        if let Some(database) = database {
            name.push_str(&database.to_string());
            name.push('.');
        }
        if let Some(table) = table {
            name.push_str(&table.to_string());
            name.push('.');
        }
        name.push_str(&column.to_string());
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_is_null(&mut self, _span: &'ast [Token<'ast>], expr: &'ast Expr<'ast>, not: bool) {
        let name = if not {
            "Function IsNotNull".to_string()
        } else {
            "Function IsNull".to_string()
        };
        self.visit_expr(expr);
        let child = self.children.pop().unwrap();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_is_distinct_from(
        &mut self,
        _span: &'ast [Token<'ast>],
        left: &'ast Expr<'ast>,
        right: &'ast Expr<'ast>,
        not: bool,
    ) {
        let name = if not {
            "Function IsNotDistinctFrom".to_string()
        } else {
            "Function IsDistinctFrom".to_string()
        };
        self.visit_expr(left);
        let child1 = self.children.pop().unwrap();
        self.visit_expr(right);
        let child2 = self.children.pop().unwrap();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![child1, child2]);
        self.children.push(node);
    }

    fn visit_in_list(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        list: &'ast [Expr<'ast>],
        not: bool,
    ) {
        self.visit_expr(expr);
        let expr_child = self.children.pop().unwrap();

        let list_format_ctx = AstFormatContext::with_children("List".to_string(), list.len());
        let mut list_children = Vec::with_capacity(list.len());
        for expr in list.iter() {
            self.visit_expr(expr);
            list_children.push(self.children.pop().unwrap());
        }
        let list_child = FormatTreeNode::with_children(list_format_ctx, list_children);

        let name = if not {
            "Function NotIn".to_string()
        } else {
            "Function In".to_string()
        };
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![expr_child, list_child]);
        self.children.push(node);
    }

    fn visit_in_subquery(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        subquery: &'ast Query<'ast>,
        not: bool,
    ) {
        self.visit_expr(expr);
        let expr_child = self.children.pop().unwrap();
        self.visit_query(subquery);
        let subquery_child = self.children.pop().unwrap();

        let name = if not {
            "Function NotInSubquery".to_string()
        } else {
            "Function InSubquery".to_string()
        };
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![expr_child, subquery_child]);
        self.children.push(node);
    }

    fn visit_between(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        low: &'ast Expr<'ast>,
        high: &'ast Expr<'ast>,
        not: bool,
    ) {
        self.visit_expr(expr);
        let expr_child = self.children.pop().unwrap();
        self.visit_expr(low);
        let low_child = self.children.pop().unwrap();
        self.visit_expr(high);
        let high_child = self.children.pop().unwrap();

        let between_format_ctx = AstFormatContext::with_children("Between".to_string(), 2);
        let between_child =
            FormatTreeNode::with_children(between_format_ctx, vec![low_child, high_child]);

        let name = if not {
            "Function NotBetween".to_string()
        } else {
            "Function Between".to_string()
        };
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![expr_child, between_child]);
        self.children.push(node);
    }

    fn visit_binary_op(
        &mut self,
        _span: &'ast [Token<'ast>],
        op: &'ast BinaryOperator,
        left: &'ast Expr<'ast>,
        right: &'ast Expr<'ast>,
    ) {
        self.visit_expr(left);
        let left_child = self.children.pop().unwrap();
        self.visit_expr(right);
        let right_child = self.children.pop().unwrap();

        let name = format!("Function {op}");
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![left_child, right_child]);
        self.children.push(node);
    }

    fn visit_unary_op(
        &mut self,
        _span: &'ast [Token<'ast>],
        op: &'ast UnaryOperator,
        expr: &'ast Expr<'ast>,
    ) {
        self.visit_expr(expr);
        let expr_child = self.children.pop().unwrap();

        let name = format!("Function {op}");
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![expr_child]);
        self.children.push(node);
    }

    fn visit_cast(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        target_type: &'ast TypeName,
        _pg_style: bool,
    ) {
        self.visit_expr(expr);
        let expr_child = self.children.pop().unwrap();
        let target_format_ctx = AstFormatContext::new(format!("TargetType {target_type}"));
        let target_child = FormatTreeNode::new(target_format_ctx);

        let name = "Function Cast".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![expr_child, target_child]);
        self.children.push(node);
    }

    fn visit_try_cast(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        target_type: &'ast TypeName,
    ) {
        self.visit_expr(expr);
        let expr_child = self.children.pop().unwrap();
        let target_format_ctx = AstFormatContext::new(format!("TargetType {target_type}"));
        let target_child = FormatTreeNode::new(target_format_ctx);

        let name = "Function TryCast".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![expr_child, target_child]);
        self.children.push(node);
    }

    fn visit_extract(
        &mut self,
        _span: &'ast [Token<'ast>],
        kind: &'ast IntervalKind,
        expr: &'ast Expr<'ast>,
    ) {
        self.visit_expr(expr);
        let expr_child = self.children.pop().unwrap();
        let kind_format_ctx = AstFormatContext::new(format!("IntervalKind {kind}"));
        let kind_child = FormatTreeNode::new(kind_format_ctx);

        let name = "Function Extract".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![expr_child, kind_child]);
        self.children.push(node);
    }

    fn visit_positon(
        &mut self,
        _span: &'ast [Token<'ast>],
        substr_expr: &'ast Expr<'ast>,
        str_expr: &'ast Expr<'ast>,
    ) {
        self.visit_expr(substr_expr);
        let substr_expr_child = self.children.pop().unwrap();
        self.visit_expr(str_expr);
        let str_expr_child = self.children.pop().unwrap();

        let name = "Function Positon".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node =
            FormatTreeNode::with_children(format_ctx, vec![substr_expr_child, str_expr_child]);
        self.children.push(node);
    }

    fn visit_substring(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        substring_from: &'ast Option<Box<Expr<'ast>>>,
        substring_for: &'ast Option<Box<Expr<'ast>>>,
    ) {
        let mut children = Vec::with_capacity(1);
        self.visit_expr(expr);
        children.push(self.children.pop().unwrap());
        if let Some(substring_from) = substring_from {
            self.visit_expr(substring_from);
            children.push(self.children.pop().unwrap());
        }
        if let Some(substring_for) = substring_for {
            self.visit_expr(substring_for);
            children.push(self.children.pop().unwrap());
        }
        let name = "Function Substring".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_trim(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        trim_where: &'ast Option<(TrimWhere, Box<Expr<'ast>>)>,
    ) {
        let mut children = Vec::with_capacity(1);
        self.visit_expr(expr);
        children.push(self.children.pop().unwrap());
        if let Some((_, trim_expr)) = trim_where {
            self.visit_expr(trim_expr);
            children.push(self.children.pop().unwrap());
        }
        let name = "Function Trim".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_literal(&mut self, _span: &'ast [Token<'ast>], lit: &'ast Literal) {
        let name = format!("Literal {:?}", lit);
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_count_all(&mut self, _span: &'ast [Token<'ast>]) {
        let name = "Function CountAll".to_string();
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_tuple(&mut self, _span: &'ast [Token<'ast>], elements: &'ast [Expr<'ast>]) {
        let mut children = Vec::with_capacity(elements.len());
        for element in elements.iter() {
            self.visit_expr(element);
            children.push(self.children.pop().unwrap());
        }
        let name = "Literal Tuple".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_function_call(
        &mut self,
        _span: &'ast [Token<'ast>],
        distinct: bool,
        name: &'ast Identifier<'ast>,
        args: &'ast [Expr<'ast>],
        _params: &'ast [Literal],
    ) {
        let mut children = Vec::with_capacity(args.len());
        for arg in args.iter() {
            self.visit_expr(arg);
            children.push(self.children.pop().unwrap());
        }
        let node_name = if distinct {
            format!("Function {name}Distinct")
        } else {
            format!("Function {name}")
        };
        let format_ctx = AstFormatContext::with_children(node_name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_case_when(
        &mut self,
        _span: &'ast [Token<'ast>],
        operand: &'ast Option<Box<Expr<'ast>>>,
        conditions: &'ast [Expr<'ast>],
        results: &'ast [Expr<'ast>],
        else_result: &'ast Option<Box<Expr<'ast>>>,
    ) {
        let mut children = Vec::new();
        if let Some(operand) = operand {
            self.visit_expr(operand);
            children.push(self.children.pop().unwrap());
        }
        if !conditions.is_empty() {
            let mut conditions_children = Vec::with_capacity(conditions.len());
            for condition in conditions.iter() {
                self.visit_expr(condition);
                conditions_children.push(self.children.pop().unwrap());
            }
            let conditions_name = "Conditions".to_string();
            let conditions_format_ctx =
                AstFormatContext::with_children(conditions_name, conditions_children.len());
            let conditions_node =
                FormatTreeNode::with_children(conditions_format_ctx, conditions_children);
            children.push(conditions_node)
        }
        if !results.is_empty() {
            let mut results_children = Vec::with_capacity(results.len());
            for result in results.iter() {
                self.visit_expr(result);
                results_children.push(self.children.pop().unwrap());
            }
            let results_name = "Results".to_string();
            let results_format_ctx =
                AstFormatContext::with_children(results_name, results_children.len());
            let results_node = FormatTreeNode::with_children(results_format_ctx, results_children);
            children.push(results_node)
        }
        if let Some(else_result) = else_result {
            self.visit_expr(else_result);
            let else_child = self.children.pop().unwrap();
            let else_name = "ElseResult".to_string();
            let else_format_ctx = AstFormatContext::with_children(else_name, 1);
            let else_node = FormatTreeNode::with_children(else_format_ctx, vec![else_child]);
            children.push(else_node)
        }

        let name = "Function Case".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_exists(&mut self, _span: &'ast [Token<'ast>], not: bool, subquery: &'ast Query<'ast>) {
        self.visit_query(subquery);
        let child = self.children.pop().unwrap();

        let name = if not {
            "Function NotExists".to_string()
        } else {
            "Function Exists".to_string()
        };
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_subquery(
        &mut self,
        _span: &'ast [Token<'ast>],
        modifier: &'ast Option<SubqueryModifier>,
        subquery: &'ast Query<'ast>,
    ) {
        self.visit_query(subquery);
        let child = self.children.pop().unwrap();

        let name = if let Some(modifier) = modifier {
            format!("Function Subquery {modifier}")
        } else {
            "Function Subquery".to_string()
        };
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_map_access(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        accessor: &'ast MapAccessor<'ast>,
    ) {
        self.visit_expr(expr);
        let expr_child = self.children.pop().unwrap();

        let key_name = match accessor {
            MapAccessor::Bracket { key } => format!("accessor [{key}]"),
            MapAccessor::Period { key } => format!("accessor .{key}"),
            MapAccessor::Colon { key } => format!("accessor :{key}"),
        };
        let key_format_ctx = AstFormatContext::new(key_name);
        let key_child = FormatTreeNode::new(key_format_ctx);

        let name = "Function MapAccess".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![expr_child, key_child]);
        self.children.push(node);
    }

    fn visit_array(&mut self, _span: &'ast [Token<'ast>], exprs: &'ast [Expr<'ast>]) {
        let mut children = Vec::with_capacity(exprs.len());
        for expr in exprs.iter() {
            self.visit_expr(expr);
            children.push(self.children.pop().unwrap());
        }
        let name = "Literal Array".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_interval(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        unit: &'ast IntervalKind,
    ) {
        self.visit_expr(expr);
        let child = self.children.pop().unwrap();

        let name = format!("Function Interval{}", unit);
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_date_add(
        &mut self,
        _span: &'ast [Token<'ast>],
        unit: &'ast IntervalKind,
        interval: &'ast Expr<'ast>,
        date: &'ast Expr<'ast>,
    ) {
        self.visit_expr(date);
        let date_child = self.children.pop().unwrap();
        self.visit_expr(interval);
        let interval_child = self.children.pop().unwrap();

        let name = format!("Function DateAdd{}", unit);
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![date_child, interval_child]);
        self.children.push(node);
    }

    fn visit_date_sub(
        &mut self,
        _span: &'ast [Token<'ast>],
        unit: &'ast IntervalKind,
        interval: &'ast Expr<'ast>,
        date: &'ast Expr<'ast>,
    ) {
        self.visit_expr(date);
        let date_child = self.children.pop().unwrap();
        self.visit_expr(interval);
        let interval_child = self.children.pop().unwrap();

        let name = format!("Function DateSub{}", unit);
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![date_child, interval_child]);
        self.children.push(node);
    }

    fn visit_date_trunc(
        &mut self,
        _span: &'ast [Token<'ast>],
        unit: &'ast IntervalKind,
        date: &'ast Expr<'ast>,
    ) {
        self.visit_expr(date);
        let child = self.children.pop().unwrap();

        let name = format!("Function DateTrunc{}", unit);
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_query(&mut self, query: &'ast Query<'ast>) {
        let mut children = Vec::new();
        if let Some(with) = &query.with {
            self.visit_with(with);
            children.push(self.children.pop().unwrap());
        }
        self.visit_set_expr(&query.body);
        children.push(self.children.pop().unwrap());
        if !query.order_by.is_empty() {
            let order_by_format_ctx =
                AstFormatContext::with_children("OrderByList".to_string(), query.order_by.len());
            let mut order_by_children = Vec::with_capacity(query.order_by.len());
            for order_by in query.order_by.iter() {
                self.visit_order_by(order_by);
                order_by_children.push(self.children.pop().unwrap());
            }
            let order_by_node =
                FormatTreeNode::with_children(order_by_format_ctx, order_by_children);
            children.push(order_by_node);
        }
        if !query.limit.is_empty() {
            let limit_format_ctx =
                AstFormatContext::with_children("LimitList".to_string(), query.limit.len());
            let mut limit_children = Vec::with_capacity(query.limit.len());
            for limit in query.limit.iter() {
                self.visit_expr(limit);
                limit_children.push(self.children.pop().unwrap());
            }
            let limit_node = FormatTreeNode::with_children(limit_format_ctx, limit_children);
            children.push(limit_node);
        }
        if let Some(offset) = &query.offset {
            self.visit_expr(offset);
            let offset_child = self.children.pop().unwrap();
            let offset_format_ctx = AstFormatContext::with_children("OffsetElement".to_string(), 1);
            let offset_node = FormatTreeNode::with_children(offset_format_ctx, vec![offset_child]);
            children.push(offset_node);
        }
        if let Some(format) = &query.format {
            let format_format_ctx = AstFormatContext::new(format!("FormatElement {}", format));
            let format_node = FormatTreeNode::new(format_format_ctx);
            children.push(format_node);
        }

        let name = "Query".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_explain(&mut self, kind: &'ast ExplainKind, query: &'ast Statement<'ast>) {
        self.visit_statement(query);
        let child = self.children.pop().unwrap();

        let name = format!("Explain{}", match kind {
            ExplainKind::Ast(_) => "Ast",
            ExplainKind::Syntax(_) => "Syntax",
            ExplainKind::Graph => "Graph",
            ExplainKind::Pipeline => "Pipeline",
            ExplainKind::Fragments => "Fragments",
            ExplainKind::Raw => "Raw",
            ExplainKind::Plan => "Plan",
        });
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_copy(&mut self, copy: &'ast CopyStmt<'ast>) {
        let mut children = Vec::new();
        self.visit_copy_unit(&copy.src);
        children.push(self.children.pop().unwrap());
        self.visit_copy_unit(&copy.dst);
        children.push(self.children.pop().unwrap());
        if !copy.files.is_empty() {
            let mut files_children = Vec::with_capacity(copy.files.len());
            for file in copy.files.iter() {
                let file_name = format!("File {}", file);
                let file_format_ctx = AstFormatContext::new(file_name);
                let file_node = FormatTreeNode::new(file_format_ctx);
                files_children.push(file_node);
            }
            let files_name = "Files".to_string();
            let files_format_ctx =
                AstFormatContext::with_children(files_name, files_children.len());
            let files_node = FormatTreeNode::with_children(files_format_ctx, files_children);
            children.push(files_node);
        }
        if !copy.pattern.is_empty() {
            let pattern_name = format!("Pattern {}", copy.pattern);
            let pattern_format_ctx = AstFormatContext::new(pattern_name);
            let pattern_node = FormatTreeNode::new(pattern_format_ctx);
            children.push(pattern_node);
        }
        if !copy.file_format.is_empty() {
            let mut file_formats_children = Vec::with_capacity(copy.file_format.len());
            for (k, v) in copy.file_format.iter() {
                let file_format_name = format!("FileFormat {} = {:?}", k, v);
                let file_format_format_ctx = AstFormatContext::new(file_format_name);
                let file_format_node = FormatTreeNode::new(file_format_format_ctx);
                file_formats_children.push(file_format_node);
            }
            let file_formats_format_name = "FileFormats".to_string();
            let files_formats_format_ctx = AstFormatContext::with_children(
                file_formats_format_name,
                file_formats_children.len(),
            );
            let files_formats_node =
                FormatTreeNode::with_children(files_formats_format_ctx, file_formats_children);
            children.push(files_formats_node);
        }
        if !copy.validation_mode.is_empty() {
            let validation_mode_name = format!("ValidationMode {}", copy.validation_mode);
            let validation_mode_format_ctx = AstFormatContext::new(validation_mode_name);
            let validation_mode_node = FormatTreeNode::new(validation_mode_format_ctx);
            children.push(validation_mode_node);
        }
        let size_limit_name = format!("SizeLimit {}", copy.size_limit);
        let size_limit_format_ctx = AstFormatContext::new(size_limit_name);
        let size_limit_node = FormatTreeNode::new(size_limit_format_ctx);
        children.push(size_limit_node);

        let name = "Copy".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_copy_unit(&mut self, copy_unit: &'ast CopyUnit<'ast>) {
        match copy_unit {
            CopyUnit::Table {
                catalog,
                database,
                table,
            } => self.visit_table_ref(catalog, database, table),
            CopyUnit::StageLocation { name, path } => {
                let location_format_ctx =
                    AstFormatContext::new(format!("Location @{}{}", name, path));
                let location_node = FormatTreeNode::new(location_format_ctx);
                self.children.push(location_node);
            }
            CopyUnit::UriLocation(v) => {
                let location_format_ctx = AstFormatContext::new(format!("UriLocation {}", v));
                let location_node = FormatTreeNode::new(location_format_ctx);
                self.children.push(location_node);
            }
            CopyUnit::Query(query) => self.visit_query(query),
        }
        let child = self.children.pop().unwrap();
        let name = "CopyUnit".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_call(&mut self, call: &'ast CallStmt) {
        let mut children = Vec::new();
        for arg in call.args.iter() {
            let arg_name = format!("Arg {}", arg);
            let arg_format_ctx = AstFormatContext::new(arg_name);
            let arg_node = FormatTreeNode::new(arg_format_ctx);
            children.push(arg_node);
        }
        let node_name = format!("Call {}", call.name);
        let format_ctx = AstFormatContext::with_children(node_name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_show_settings(&mut self, like: &'ast Option<String>) {
        let mut children = Vec::new();
        if let Some(like) = like {
            let like_name = format!("Like {}", like);
            let like_format_ctx = AstFormatContext::new(like_name);
            let like_node = FormatTreeNode::new(like_format_ctx);
            children.push(like_node);
        }
        let name = "ShowSetting".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_show_process_list(&mut self) {
        let name = "ShowProcessList".to_string();
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_show_metrics(&mut self) {
        let name = "ShowMetrics".to_string();
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_show_engines(&mut self) {
        let name = "ShowEngines".to_string();
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_show_functions(&mut self, limit: &'ast Option<ShowLimit<'ast>>) {
        let mut children = Vec::new();
        if let Some(limit) = limit {
            self.visit_show_limit(limit);
            children.push(self.children.pop().unwrap());
        }
        let name = "ShowFunctions".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_show_limit(&mut self, limit: &'ast ShowLimit<'ast>) {
        match limit {
            ShowLimit::Like { pattern } => {
                let name = format!("LimitLike {}", pattern);
                let format_ctx = AstFormatContext::new(name);
                let node = FormatTreeNode::new(format_ctx);
                self.children.push(node);
            }
            ShowLimit::Where { selection } => {
                self.visit_expr(selection);
                let child = self.children.pop().unwrap();
                let name = "LimitWhere".to_string();
                let format_ctx = AstFormatContext::with_children(name, 1);
                let node = FormatTreeNode::with_children(format_ctx, vec![child]);
                self.children.push(node);
            }
        }
    }

    fn visit_kill(&mut self, kill_target: &'ast KillTarget, object_id: &'ast str) {
        let name = format!("Kill {} {}", kill_target, object_id);
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_set_variable(
        &mut self,
        is_global: bool,
        variable: &'ast Identifier<'ast>,
        value: &'ast Literal,
    ) {
        let name = if is_global {
            format!("SetGlobal {} = {}", variable, value)
        } else {
            format!("Set {} = {}", variable, value)
        };
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_insert(&mut self, insert: &'ast InsertStmt<'ast>) {
        let mut children = Vec::new();
        self.visit_table_ref(&insert.catalog, &insert.database, &insert.table);
        children.push(self.children.pop().unwrap());
        if !insert.columns.is_empty() {
            let mut columns_children = Vec::with_capacity(insert.columns.len());
            for column in insert.columns.iter() {
                self.visit_identifier(column);
                columns_children.push(self.children.pop().unwrap());
            }
            let columns_name = "Columns".to_string();
            let columns_format_ctx =
                AstFormatContext::with_children(columns_name, columns_children.len());
            let columns_node = FormatTreeNode::with_children(columns_format_ctx, columns_children);
            children.push(columns_node);
        }
        self.visit_insert_source(&insert.source);
        children.push(self.children.pop().unwrap());

        let name = if insert.overwrite {
            "InsertOverwrite".to_string()
        } else {
            "Insert".to_string()
        };
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_insert_source(&mut self, insert_source: &'ast InsertSource<'ast>) {
        match insert_source {
            InsertSource::Streaming { format, .. } => {
                let streaming_name = format!("StreamSouce {}", format);
                let streaming_format_ctx = AstFormatContext::new(streaming_name);
                let streaming_node = FormatTreeNode::new(streaming_format_ctx);
                self.children.push(streaming_node);
            }
            InsertSource::Values { .. } => {
                let values_name = "ValueSouce".to_string();
                let values_format_ctx = AstFormatContext::new(values_name);
                let values_node = FormatTreeNode::new(values_format_ctx);
                self.children.push(values_node);
            }
            InsertSource::Select { query } => self.visit_query(query),
        }
        let child = self.children.pop().unwrap();
        let name = "Source".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_delete(
        &mut self,
        table_reference: &'ast TableReference<'ast>,
        selection: &'ast Option<Expr<'ast>>,
    ) {
        let mut children = Vec::new();
        self.visit_table_reference(table_reference);
        children.push(self.children.pop().unwrap());
        if let Some(selection) = selection {
            self.visit_expr(selection);
            children.push(self.children.pop().unwrap());
        }

        let name = "Delete".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_show_databases(&mut self, stmt: &'ast ShowDatabasesStmt<'ast>) {
        let mut children = Vec::new();
        if let Some(limit) = &stmt.limit {
            self.visit_show_limit(limit);
            children.push(self.children.pop().unwrap());
        }
        let name = "ShowDatabases".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_show_create_databases(&mut self, stmt: &'ast ShowCreateDatabaseStmt<'ast>) {
        self.visit_database_ref(&stmt.catalog, &stmt.database);
        let child = self.children.pop().unwrap();
        let name = "ShowCreateDatabase".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_create_database(&mut self, stmt: &'ast CreateDatabaseStmt<'ast>) {
        let mut children = Vec::new();
        self.visit_database_ref(&stmt.catalog, &stmt.database);
        children.push(self.children.pop().unwrap());
        if let Some(engine) = &stmt.engine {
            let engine_name = format!("DatabaseEngine {}", engine);
            let engine_format_ctx = AstFormatContext::new(engine_name);
            let engine_node = FormatTreeNode::new(engine_format_ctx);
            children.push(engine_node);
        }
        if !stmt.options.is_empty() {
            let mut options_children = Vec::with_capacity(stmt.options.len());
            for option in stmt.options.iter() {
                let option_name = format!("DatabaseOption {} = {:?}", option.name, option.value);
                let option_format_ctx = AstFormatContext::new(option_name);
                let option_format_node = FormatTreeNode::new(option_format_ctx);
                options_children.push(option_format_node);
            }
            let options_format_name = "DatabaseOptions".to_string();
            let options_format_ctx =
                AstFormatContext::with_children(options_format_name, options_children.len());
            let options_node = FormatTreeNode::with_children(options_format_ctx, options_children);
            children.push(options_node);
        }
        let name = "CreateDatabase".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_drop_database(&mut self, stmt: &'ast DropDatabaseStmt<'ast>) {
        self.visit_database_ref(&stmt.catalog, &stmt.database);
        let child = self.children.pop().unwrap();
        let name = "DropDatabase".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_undrop_database(&mut self, stmt: &'ast UndropDatabaseStmt<'ast>) {
        self.visit_database_ref(&stmt.catalog, &stmt.database);
        let child = self.children.pop().unwrap();
        let name = "UndropDatabase".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_alter_database(&mut self, stmt: &'ast AlterDatabaseStmt<'ast>) {
        self.visit_database_ref(&stmt.catalog, &stmt.database);
        let database_child = self.children.pop().unwrap();

        let action_child = match &stmt.action {
            AlterDatabaseAction::RenameDatabase { new_db } => {
                let action_name = format!("Action RenameTo {}", new_db);
                let action_format_ctx = AstFormatContext::new(action_name);
                FormatTreeNode::new(action_format_ctx)
            }
        };

        let name = "AlterDatabase".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![database_child, action_child]);
        self.children.push(node);
    }

    fn visit_use_database(&mut self, database: &'ast Identifier<'ast>) {
        self.visit_identifier(database);
        let child = self.children.pop().unwrap();
        let name = "UseDatabase".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_show_tables(&mut self, stmt: &'ast ShowTablesStmt<'ast>) {
        let mut children = Vec::new();
        if let Some(database) = &stmt.database {
            let database_name = format!("Database {}", database);
            let database_format_ctx = AstFormatContext::new(database_name);
            let database_node = FormatTreeNode::new(database_format_ctx);
            children.push(database_node);
        }
        if let Some(limit) = &stmt.limit {
            self.visit_show_limit(limit);
            children.push(self.children.pop().unwrap());
        }
        let name = "ShowTables".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_show_create_table(&mut self, stmt: &'ast ShowCreateTableStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        let child = self.children.pop().unwrap();
        let name = "ShowCreateTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_describe_table(&mut self, stmt: &'ast DescribeTableStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        let child = self.children.pop().unwrap();
        let name = "DescribeTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_show_tables_status(&mut self, stmt: &'ast ShowTablesStatusStmt<'ast>) {
        let mut children = Vec::new();
        if let Some(database) = &stmt.database {
            let database_name = format!("Database {}", database);
            let database_format_ctx = AstFormatContext::new(database_name);
            let database_node = FormatTreeNode::new(database_format_ctx);
            children.push(database_node);
        }
        if let Some(limit) = &stmt.limit {
            self.visit_show_limit(limit);
            children.push(self.children.pop().unwrap());
        }
        let name = "ShowTablesStatus".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_create_table(&mut self, stmt: &'ast CreateTableStmt<'ast>) {
        let mut children = Vec::new();
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        children.push(self.children.pop().unwrap());
        if let Some(source) = &stmt.source {
            self.visit_create_table_source(source);
            children.push(self.children.pop().unwrap());
        }
        if let Some(engine) = &stmt.engine {
            let engine_name = format!("TableEngine {}", engine);
            let engine_format_ctx = AstFormatContext::new(engine_name);
            let engine_node = FormatTreeNode::new(engine_format_ctx);
            children.push(engine_node);
        }
        if !stmt.cluster_by.is_empty() {
            let mut cluster_by_children = Vec::with_capacity(stmt.cluster_by.len());
            for cluster_by in stmt.cluster_by.iter() {
                self.visit_expr(cluster_by);
                cluster_by_children.push(self.children.pop().unwrap());
            }
            let cluster_by_name = "ClusterByList".to_string();
            let cluster_by_format_ctx =
                AstFormatContext::with_children(cluster_by_name, cluster_by_children.len());
            let cluster_by_node =
                FormatTreeNode::with_children(cluster_by_format_ctx, cluster_by_children);
            children.push(cluster_by_node);
        }
        if !stmt.table_options.is_empty() {
            let mut table_options_children = Vec::with_capacity(stmt.table_options.len());
            for (k, v) in stmt.table_options.iter() {
                let table_option_name = format!("TableOption {} = {:?}", k, v);
                let table_option_format_ctx = AstFormatContext::new(table_option_name);
                let table_option_node = FormatTreeNode::new(table_option_format_ctx);
                table_options_children.push(table_option_node);
            }
            let table_options_format_name = "TableOptions".to_string();
            let table_options_format_ctx = AstFormatContext::with_children(
                table_options_format_name,
                table_options_children.len(),
            );
            let table_options_node =
                FormatTreeNode::with_children(table_options_format_ctx, table_options_children);
            children.push(table_options_node);
        }
        if let Some(as_query) = &stmt.as_query {
            self.visit_query(as_query);
            children.push(self.children.pop().unwrap());
        }
        let name = "CreateTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_create_table_source(&mut self, source: &'ast CreateTableSource<'ast>) {
        match source {
            CreateTableSource::Columns(columns) => {
                let mut children = Vec::with_capacity(columns.len());
                for column in columns.iter() {
                    self.visit_column_definition(column);
                    children.push(self.children.pop().unwrap());
                }
                let name = "ColumnsDefinition".to_string();
                let format_ctx = AstFormatContext::with_children(name, children.len());
                let node = FormatTreeNode::with_children(format_ctx, children);
                self.children.push(node);
            }
            CreateTableSource::Like {
                catalog,
                database,
                table,
            } => {
                self.visit_table_ref(catalog, database, table);
                let child = self.children.pop().unwrap();
                let name = "LikeTable".to_string();
                let format_ctx = AstFormatContext::with_children(name, 1);
                let node = FormatTreeNode::with_children(format_ctx, vec![child]);
                self.children.push(node);
            }
        }
    }

    fn visit_column_definition(&mut self, column_definition: &'ast ColumnDefinition<'ast>) {
        let type_name = format!("DataType {}", column_definition.data_type);
        let type_format_ctx = AstFormatContext::new(type_name);
        let type_node = FormatTreeNode::new(type_format_ctx);

        let name = format!("ColumnDefinition {}", column_definition.name);
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![type_node]);
        self.children.push(node);
    }

    fn visit_drop_table(&mut self, stmt: &'ast DropTableStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        let child = self.children.pop().unwrap();

        let name = "DropTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_undrop_table(&mut self, stmt: &'ast UndropTableStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        let child = self.children.pop().unwrap();

        let name = "UndropTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_alter_table(&mut self, stmt: &'ast AlterTableStmt<'ast>) {
        self.visit_table_reference(&stmt.table_reference);
        let table_child = self.children.pop().unwrap();

        let action_child = match &stmt.action {
            AlterTableAction::RenameTable { new_table } => {
                let action_name = format!("Action RenameTo {}", new_table);
                let action_format_ctx = AstFormatContext::new(action_name);
                FormatTreeNode::new(action_format_ctx)
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                let mut cluster_by_children = Vec::with_capacity(cluster_by.len());
                for cluster_by_expr in cluster_by.iter() {
                    self.visit_expr(cluster_by_expr);
                    cluster_by_children.push(self.children.pop().unwrap());
                }
                let cluster_by_name = "Action ClusterByList".to_string();
                let cluster_by_format_ctx =
                    AstFormatContext::with_children(cluster_by_name, cluster_by_children.len());
                FormatTreeNode::with_children(cluster_by_format_ctx, cluster_by_children)
            }
            AlterTableAction::DropTableClusterKey => {
                let action_name = "Action DropClusterKey".to_string();
                let action_format_ctx = AstFormatContext::new(action_name);
                FormatTreeNode::new(action_format_ctx)
            }
            AlterTableAction::ReclusterTable { selection, .. } => {
                let mut children = Vec::new();
                if let Some(selection) = selection {
                    self.visit_expr(selection);
                    children.push(self.children.pop().unwrap());
                }
                let action_name = "Action Recluster".to_string();
                let action_format_ctx =
                    AstFormatContext::with_children(action_name, children.len());
                FormatTreeNode::with_children(action_format_ctx, children)
            }
        };

        let name = "AlterTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![table_child, action_child]);
        self.children.push(node);
    }

    fn visit_rename_table(&mut self, stmt: &'ast RenameTableStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        let old_child = self.children.pop().unwrap();
        self.visit_table_ref(&stmt.new_catalog, &stmt.new_database, &stmt.new_table);
        let new_child = self.children.pop().unwrap();

        let name = "RenameTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![old_child, new_child]);
        self.children.push(node);
    }

    fn visit_truncate_table(&mut self, stmt: &'ast TruncateTableStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        let child = self.children.pop().unwrap();

        let name = "TruncateTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_optimize_table(&mut self, stmt: &'ast OptimizeTableStmt<'ast>) {
        let mut children = Vec::new();
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        children.push(self.children.pop().unwrap());
        if let Some(action) = stmt.action {
            let action_name = format!("Action {}", action);
            let action_format_ctx = AstFormatContext::new(action_name);
            children.push(FormatTreeNode::new(action_format_ctx));
        }

        let name = "OptimizeTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_exists_table(&mut self, stmt: &'ast ExistsTableStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.table);
        let child = self.children.pop().unwrap();

        let name = "ExistsTable".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_create_view(&mut self, stmt: &'ast CreateViewStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.view);
        let view_child = self.children.pop().unwrap();
        self.visit_query(&*stmt.query);
        let query_child = self.children.pop().unwrap();

        let name = "CreateView".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![view_child, query_child]);
        self.children.push(node);
    }

    fn visit_alter_view(&mut self, stmt: &'ast AlterViewStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.view);
        let view_child = self.children.pop().unwrap();
        self.visit_query(&*stmt.query);
        let query_child = self.children.pop().unwrap();

        let name = "AlterView".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![view_child, query_child]);
        self.children.push(node);
    }

    fn visit_drop_view(&mut self, stmt: &'ast DropViewStmt<'ast>) {
        self.visit_table_ref(&stmt.catalog, &stmt.database, &stmt.view);
        let child = self.children.pop().unwrap();

        let name = "DropView".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_show_users(&mut self) {
        let name = "ShowUsers".to_string();
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_create_user(&mut self, stmt: &'ast CreateUserStmt) {
        let mut children = Vec::new();
        let user_name = format!("User {}", stmt.user);
        let user_format_ctx = AstFormatContext::new(user_name);
        children.push(FormatTreeNode::new(user_format_ctx));
        if let Some(auth_type) = &stmt.auth_option.auth_type {
            let auth_type_name = format!("AuthType {}", auth_type.to_str());
            let auth_type_format_ctx = AstFormatContext::new(auth_type_name);
            children.push(FormatTreeNode::new(auth_type_format_ctx));
        }
        if let Some(password) = &stmt.auth_option.password {
            let auth_password_name = format!("Password {:?}", password);
            let auth_password_format_ctx = AstFormatContext::new(auth_password_name);
            children.push(FormatTreeNode::new(auth_password_format_ctx));
        }
        if !stmt.user_options.is_empty() {
            let mut user_options_children = Vec::with_capacity(stmt.user_options.len());
            for user_option in stmt.user_options.iter() {
                let user_option_name = format!("UserOption {}", user_option);
                let user_option_format_ctx = AstFormatContext::new(user_option_name);
                let user_option_node = FormatTreeNode::new(user_option_format_ctx);
                user_options_children.push(user_option_node);
            }
            let user_options_format_name = "UserOptions".to_string();
            let user_options_format_ctx = AstFormatContext::with_children(
                user_options_format_name,
                user_options_children.len(),
            );
            let user_options_node =
                FormatTreeNode::with_children(user_options_format_ctx, user_options_children);
            children.push(user_options_node);
        }
        let name = "CreateUser".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_alter_user(&mut self, stmt: &'ast AlterUserStmt) {
        let mut children = Vec::new();
        if let Some(user) = &stmt.user {
            let user_name = format!("User {}", user);
            let user_format_ctx = AstFormatContext::new(user_name);
            children.push(FormatTreeNode::new(user_format_ctx));
        }
        if let Some(auth_option) = &stmt.auth_option {
            if let Some(auth_type) = &auth_option.auth_type {
                let auth_type_name = format!("AuthType {}", auth_type.to_str());
                let auth_type_format_ctx = AstFormatContext::new(auth_type_name);
                children.push(FormatTreeNode::new(auth_type_format_ctx));
            }
            if let Some(password) = &auth_option.password {
                let auth_password_name = format!("Password {}", password);
                let auth_password_format_ctx = AstFormatContext::new(auth_password_name);
                children.push(FormatTreeNode::new(auth_password_format_ctx));
            }
        }
        if !stmt.user_options.is_empty() {
            let mut user_options_children = Vec::with_capacity(stmt.user_options.len());
            for user_option in stmt.user_options.iter() {
                let user_option_name = format!("UserOption {}", user_option);
                let user_option_format_ctx = AstFormatContext::new(user_option_name);
                let user_option_node = FormatTreeNode::new(user_option_format_ctx);
                user_options_children.push(user_option_node);
            }
            let user_options_format_name = "UserOptions".to_string();
            let user_options_format_ctx = AstFormatContext::with_children(
                user_options_format_name,
                user_options_children.len(),
            );
            let user_options_node =
                FormatTreeNode::with_children(user_options_format_ctx, user_options_children);
            children.push(user_options_node);
        }
        let name = "AlterUser".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_drop_user(&mut self, _if_exists: bool, user: &'ast UserIdentity) {
        let user_name = format!("User {}", user);
        let user_format_ctx = AstFormatContext::new(user_name);
        let child = FormatTreeNode::new(user_format_ctx);

        let name = "DropUser".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_show_roles(&mut self) {
        let name = "ShowRoles".to_string();
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_create_role(&mut self, _if_not_exists: bool, role_name: &'ast str) {
        let role_name = format!("Role {}", role_name);
        let role_format_ctx = AstFormatContext::new(role_name);
        let child = FormatTreeNode::new(role_format_ctx);

        let name = "CreateRole".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_drop_role(&mut self, _if_exists: bool, role_name: &'ast str) {
        let role_name = format!("Role {}", role_name);
        let role_format_ctx = AstFormatContext::new(role_name);
        let child = FormatTreeNode::new(role_format_ctx);

        let name = "DropRole".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_grant(&mut self, grant: &'ast GrantStmt) {
        let source_child = match &grant.source {
            AccountMgrSource::Role { role } => {
                let role_name = format!("Role {}", role);
                let role_format_ctx = AstFormatContext::new(role_name);
                FormatTreeNode::new(role_format_ctx)
            }
            AccountMgrSource::Privs { privileges, .. } => {
                let mut privileges_children = Vec::with_capacity(privileges.len());
                for privilege in privileges.iter() {
                    let privilege_name = format!("Privilege {}", privilege);
                    let privilege_format_ctx = AstFormatContext::new(privilege_name);
                    privileges_children.push(FormatTreeNode::new(privilege_format_ctx));
                }
                let privileges_name = "Privileges".to_string();
                let privileges_format_ctx =
                    AstFormatContext::with_children(privileges_name, privileges_children.len());
                FormatTreeNode::with_children(privileges_format_ctx, privileges_children)
            }
            AccountMgrSource::ALL { .. } => {
                let all_name = "All".to_string();
                let all_format_ctx = AstFormatContext::new(all_name);
                FormatTreeNode::new(all_format_ctx)
            }
        };
        let principal_name = match &grant.principal {
            PrincipalIdentity::User(user) => format!("User {}", user),
            PrincipalIdentity::Role(role) => format!("Role {}", role),
        };
        let principal_format_ctx = AstFormatContext::new(principal_name);
        let principal_child = FormatTreeNode::new(principal_format_ctx);

        let name = "Grant".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![source_child, principal_child]);
        self.children.push(node);
    }

    fn visit_show_grant(&mut self, principal: &'ast Option<PrincipalIdentity>) {
        let mut children = Vec::new();
        if let Some(principal) = &principal {
            let principal_name = match principal {
                PrincipalIdentity::User(user) => format!("User {}", user),
                PrincipalIdentity::Role(role) => format!("Role {}", role),
            };
            let principal_format_ctx = AstFormatContext::new(principal_name);
            children.push(FormatTreeNode::new(principal_format_ctx));
        }
        let name = "ShowGrant".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_revoke(&mut self, revoke: &'ast RevokeStmt) {
        let source_child = match &revoke.source {
            AccountMgrSource::Role { role } => {
                let role_name = format!("Role {}", role);
                let role_format_ctx = AstFormatContext::new(role_name);
                FormatTreeNode::new(role_format_ctx)
            }
            AccountMgrSource::Privs { privileges, .. } => {
                let mut privileges_children = Vec::with_capacity(privileges.len());
                for privilege in privileges.iter() {
                    let privilege_name = format!("Privilege {}", privilege);
                    let privilege_format_ctx = AstFormatContext::new(privilege_name);
                    privileges_children.push(FormatTreeNode::new(privilege_format_ctx));
                }
                let privileges_name = "Privileges".to_string();
                let privileges_format_ctx =
                    AstFormatContext::with_children(privileges_name, privileges_children.len());
                FormatTreeNode::with_children(privileges_format_ctx, privileges_children)
            }
            AccountMgrSource::ALL { .. } => {
                let all_name = "All".to_string();
                let all_format_ctx = AstFormatContext::new(all_name);
                FormatTreeNode::new(all_format_ctx)
            }
        };
        let principal_name = match &revoke.principal {
            PrincipalIdentity::User(user) => format!("User {}", user),
            PrincipalIdentity::Role(role) => format!("Role {}", role),
        };
        let principal_format_ctx = AstFormatContext::new(principal_name);
        let principal_child = FormatTreeNode::new(principal_format_ctx);

        let name = "Revoke".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![source_child, principal_child]);
        self.children.push(node);
    }

    fn visit_create_udf(
        &mut self,
        _if_not_exists: bool,
        udf_name: &'ast Identifier<'ast>,
        parameters: &'ast [Identifier<'ast>],
        definition: &'ast Expr<'ast>,
        description: &'ast Option<String>,
    ) {
        let mut children = Vec::new();
        let udf_name_format_ctx = AstFormatContext::new(format!("UdfNameIdentifier {}", udf_name));
        children.push(FormatTreeNode::new(udf_name_format_ctx));
        if !parameters.is_empty() {
            let mut parameters_children = Vec::with_capacity(parameters.len());
            for parameter in parameters.iter() {
                self.visit_identifier(parameter);
                parameters_children.push(self.children.pop().unwrap());
            }
            let parameters_name = "UdfParameters".to_string();
            let parameters_format_ctx =
                AstFormatContext::with_children(parameters_name, parameters_children.len());
            children.push(FormatTreeNode::with_children(
                parameters_format_ctx,
                parameters_children,
            ));
        }
        self.visit_expr(definition);
        let definition_child = self.children.pop().unwrap();
        let definition_name = "UdfDefinition".to_string();
        let definition_format_ctx = AstFormatContext::with_children(definition_name, 1);
        children.push(FormatTreeNode::with_children(definition_format_ctx, vec![
            definition_child,
        ]));
        if let Some(description) = description {
            let description_name = format!("UdfDescription {}", description);
            let description_format_ctx = AstFormatContext::new(description_name);
            children.push(FormatTreeNode::new(description_format_ctx));
        }

        let name = "CreateUdf".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_drop_udf(&mut self, _if_exists: bool, udf_name: &'ast Identifier<'ast>) {
        let udf_name_format_ctx = AstFormatContext::new(format!("UdfIdentifier {}", udf_name));
        let child = FormatTreeNode::new(udf_name_format_ctx);

        let name = "DropUdf".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_alter_udf(
        &mut self,
        udf_name: &'ast Identifier<'ast>,
        parameters: &'ast [Identifier<'ast>],
        definition: &'ast Expr<'ast>,
        description: &'ast Option<String>,
    ) {
        let mut children = Vec::new();
        let udf_name_format_ctx = AstFormatContext::new(format!("UdfNameIdentifier {}", udf_name));
        children.push(FormatTreeNode::new(udf_name_format_ctx));
        if !parameters.is_empty() {
            let mut parameters_children = Vec::with_capacity(parameters.len());
            for parameter in parameters.iter() {
                self.visit_identifier(parameter);
                parameters_children.push(self.children.pop().unwrap());
            }
            let parameters_name = "UdfParameters".to_string();
            let parameters_format_ctx =
                AstFormatContext::with_children(parameters_name, parameters_children.len());
            children.push(FormatTreeNode::with_children(
                parameters_format_ctx,
                parameters_children,
            ));
        }
        self.visit_expr(definition);
        let definition_child = self.children.pop().unwrap();
        let definition_name = "UdfDefinition".to_string();
        let definition_format_ctx = AstFormatContext::with_children(definition_name, 1);
        children.push(FormatTreeNode::with_children(definition_format_ctx, vec![
            definition_child,
        ]));
        if let Some(description) = description {
            let description_name = format!("UdfDescription {}", description);
            let description_format_ctx = AstFormatContext::new(description_name);
            children.push(FormatTreeNode::new(description_format_ctx));
        }

        let name = "AlterUdf".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_create_stage(&mut self, stmt: &'ast CreateStageStmt) {
        let mut children = Vec::new();
        let stage_name_format_ctx = AstFormatContext::new(format!("StageName {}", stmt.stage_name));
        children.push(FormatTreeNode::new(stage_name_format_ctx));
        if let Some(location) = &stmt.location {
            let location_name = format!("Location {}", location);
            let location_format_ctx = AstFormatContext::new(location_name);
            children.push(FormatTreeNode::new(location_format_ctx));
        }
        if !stmt.file_format_options.is_empty() {
            let mut file_formats_children = Vec::with_capacity(stmt.file_format_options.len());
            for (k, v) in stmt.file_format_options.iter() {
                let file_format_name = format!("FileFormat {} = {:?}", k, v);
                let file_format_format_ctx = AstFormatContext::new(file_format_name);
                let file_format_node = FormatTreeNode::new(file_format_format_ctx);
                file_formats_children.push(file_format_node);
            }
            let file_formats_format_name = "FileFormats".to_string();
            let files_formats_format_ctx = AstFormatContext::with_children(
                file_formats_format_name,
                file_formats_children.len(),
            );
            let files_formats_node =
                FormatTreeNode::with_children(files_formats_format_ctx, file_formats_children);
            children.push(files_formats_node);
        }
        if !stmt.on_error.is_empty() {
            let on_error_name = format!("OnError {}", stmt.on_error);
            let on_error_format_ctx = AstFormatContext::new(on_error_name);
            children.push(FormatTreeNode::new(on_error_format_ctx));
        }
        let size_limit_name = format!("SizeLimit {}", stmt.size_limit);
        let size_limit_format_ctx = AstFormatContext::new(size_limit_name);
        children.push(FormatTreeNode::new(size_limit_format_ctx));
        if !stmt.validation_mode.is_empty() {
            let validation_mode_name = format!("ValidationMode {}", stmt.validation_mode);
            let validation_mode_format_ctx = AstFormatContext::new(validation_mode_name);
            children.push(FormatTreeNode::new(validation_mode_format_ctx));
        }
        if !stmt.comments.is_empty() {
            let comments_name = format!("Comments {}", stmt.comments);
            let comments_format_ctx = AstFormatContext::new(comments_name);
            children.push(FormatTreeNode::new(comments_format_ctx));
        }

        let name = "CreateStage".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_show_stages(&mut self) {
        let name = "ShowStages".to_string();
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_drop_stage(&mut self, _if_exists: bool, stage_name: &'ast str) {
        let stage_name_format_ctx = AstFormatContext::new(format!("StageName {}", stage_name));
        let child = FormatTreeNode::new(stage_name_format_ctx);

        let name = "DropStage".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_describe_stage(&mut self, stage_name: &'ast str) {
        let stage_name_format_ctx = AstFormatContext::new(format!("StageName {}", stage_name));
        let child = FormatTreeNode::new(stage_name_format_ctx);

        let name = "DescribeStage".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_remove_stage(&mut self, location: &'ast str, pattern: &'ast str) {
        let location_format_ctx = AstFormatContext::new(format!("Location {}", location));
        let location_child = FormatTreeNode::new(location_format_ctx);
        let pattern_format_ctx = AstFormatContext::new(format!("Pattern {}", pattern));
        let pattern_child = FormatTreeNode::new(pattern_format_ctx);

        let name = "RemoveStage".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![location_child, pattern_child]);
        self.children.push(node);
    }

    fn visit_list_stage(&mut self, location: &'ast str, pattern: &'ast str) {
        let location_format_ctx = AstFormatContext::new(format!("Location {}", location));
        let location_child = FormatTreeNode::new(location_format_ctx);
        let pattern_format_ctx = AstFormatContext::new(format!("Pattern {}", pattern));
        let pattern_child = FormatTreeNode::new(pattern_format_ctx);

        let name = "ListStage".to_string();
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![location_child, pattern_child]);
        self.children.push(node);
    }

    fn visit_presign(&mut self, presign: &'ast PresignStmt) {
        let mut children = Vec::with_capacity(3);
        let action_format_ctx = AstFormatContext::new(format!("Action {}", presign.action));
        children.push(FormatTreeNode::new(action_format_ctx));
        let location_format_ctx = AstFormatContext::new(format!("Location {}", presign.location));
        children.push(FormatTreeNode::new(location_format_ctx));
        let expire_format_ctx = AstFormatContext::new(format!("Expire {:?}", presign.expire));
        children.push(FormatTreeNode::new(expire_format_ctx));

        let name = "Presign".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_create_share(&mut self, stmt: &'ast CreateShareStmt<'ast>) {
        let mut children = Vec::new();
        let share_format_ctx = AstFormatContext::new(format!("ShareIdentifier {}", stmt.share));
        children.push(FormatTreeNode::new(share_format_ctx));
        if let Some(comment) = &stmt.comment {
            let comment_format_ctx = AstFormatContext::new(format!("Comment {}", comment));
            children.push(FormatTreeNode::new(comment_format_ctx));
        }

        let name = "CreateShare".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_drop_share(&mut self, stmt: &'ast DropShareStmt<'ast>) {
        let share_format_ctx = AstFormatContext::new(format!("ShareIdentifier {}", stmt.share));
        let child = FormatTreeNode::new(share_format_ctx);

        let name = "DropShare".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_grant_share_object(&mut self, stmt: &'ast GrantShareObjectStmt<'ast>) {
        let mut children = Vec::new();
        let share_format_ctx = AstFormatContext::new(format!("ShareIdentifier {}", stmt.share));
        children.push(FormatTreeNode::new(share_format_ctx));
        let object_format_ctx = AstFormatContext::new(format!("Object {}", stmt.object));
        children.push(FormatTreeNode::new(object_format_ctx));
        let privilege_format_ctx = AstFormatContext::new(format!("Privilege {}", stmt.privilege));
        children.push(FormatTreeNode::new(privilege_format_ctx));

        let name = "GrantShareObject".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_revoke_share_object(&mut self, stmt: &'ast RevokeShareObjectStmt<'ast>) {
        let mut children = Vec::new();
        let share_format_ctx = AstFormatContext::new(format!("ShareIdentifier {}", stmt.share));
        children.push(FormatTreeNode::new(share_format_ctx));
        let object_format_ctx = AstFormatContext::new(format!("Object {}", stmt.object));
        children.push(FormatTreeNode::new(object_format_ctx));
        let privilege_format_ctx = AstFormatContext::new(format!("Privilege {}", stmt.privilege));
        children.push(FormatTreeNode::new(privilege_format_ctx));

        let name = "RevokeShareObject".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_alter_share_tenants(&mut self, stmt: &'ast AlterShareTenantsStmt<'ast>) {
        let mut children = Vec::new();
        let share_format_ctx = AstFormatContext::new(format!("ShareIdentifier {}", stmt.share));
        children.push(FormatTreeNode::new(share_format_ctx));
        if !stmt.tenants.is_empty() {
            let mut tenants_children = Vec::with_capacity(stmt.tenants.len());
            for tenant in stmt.tenants.iter() {
                self.visit_identifier(tenant);
                tenants_children.push(self.children.pop().unwrap());
            }
            let tenants_name = "Tenants".to_string();
            let tenants_format_ctx =
                AstFormatContext::with_children(tenants_name, tenants_children.len());
            let tenants_node = FormatTreeNode::with_children(tenants_format_ctx, tenants_children);
            children.push(tenants_node);
        }

        let name = "AlterShareTenants".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_desc_share(&mut self, stmt: &'ast DescShareStmt<'ast>) {
        let share_format_ctx = AstFormatContext::new(format!("ShareIdentifier {}", stmt.share));
        let child = FormatTreeNode::new(share_format_ctx);

        let name = "DescShare".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_show_shares(&mut self, _stmt: &'ast ShowSharesStmt) {
        let name = "ShowShares".to_string();
        let format_ctx = AstFormatContext::new(name);
        let node = FormatTreeNode::new(format_ctx);
        self.children.push(node);
    }

    fn visit_show_object_grant_privileges(&mut self, stmt: &'ast ShowObjectGrantPrivilegesStmt) {
        let object_format_ctx = AstFormatContext::new(format!("Object {}", stmt.object));
        let child = FormatTreeNode::new(object_format_ctx);

        let name = "ShowObjectGrantPrivileges".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_show_grants_of_share(&mut self, stmt: &'ast ShowGrantsOfShareStmt) {
        let share_format_ctx = AstFormatContext::new(format!("ShareName {}", stmt.share_name));
        let child = FormatTreeNode::new(share_format_ctx);

        let name = "ShowGrantsOfShare".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_with(&mut self, with: &'ast With<'ast>) {
        let mut children = Vec::with_capacity(with.ctes.len());
        for cte in with.ctes.iter() {
            self.visit_query(&cte.query);
            let query_child = self.children.pop().unwrap();
            let cte_format_ctx = AstFormatContext::with_children_alias(
                "CTE".to_string(),
                1,
                Some(format!("{}", cte.alias)),
            );
            let cte_node = FormatTreeNode::with_children(cte_format_ctx, vec![query_child]);
            children.push(cte_node);
        }

        let name = "With".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_set_expr(&mut self, expr: &'ast SetExpr<'ast>) {
        match expr {
            SetExpr::Select(select_stmt) => self.visit_select_stmt(select_stmt),
            SetExpr::Query(query) => self.visit_query(query),
            SetExpr::SetOperation(set_operation) => self.visit_set_operation(set_operation),
        }
        let child = self.children.pop().unwrap();

        let name = "QueryBody".to_string();
        let format_ctx = AstFormatContext::with_children(name, 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_set_operation(&mut self, set_operation: &'ast SetOperation<'ast>) {
        self.visit_set_expr(&*set_operation.left);
        let left_child = self.children.pop().unwrap();
        self.visit_set_expr(&*set_operation.right);
        let right_child = self.children.pop().unwrap();

        let name = format!("SetOperation {}", match set_operation.op {
            SetOperator::Union => "Union",
            SetOperator::Except => "Except",
            SetOperator::Intersect => "Intersect",
        });
        let format_ctx = AstFormatContext::with_children(name, 2);
        let node = FormatTreeNode::with_children(format_ctx, vec![left_child, right_child]);
        self.children.push(node);
    }

    fn visit_order_by(&mut self, order_by: &'ast OrderByExpr<'ast>) {
        self.visit_expr(&order_by.expr);
        let child = self.children.pop().unwrap();
        let format_ctx = AstFormatContext::with_children("OrderByElement".to_string(), 1);
        let node = FormatTreeNode::with_children(format_ctx, vec![child]);
        self.children.push(node);
    }

    fn visit_select_stmt(&mut self, stmt: &'ast SelectStmt<'ast>) {
        let mut children = Vec::new();
        if !stmt.select_list.is_empty() {
            let mut select_list_children = Vec::with_capacity(stmt.select_list.len());
            for select_target in stmt.select_list.iter() {
                self.visit_select_target(select_target);
                select_list_children.push(self.children.pop().unwrap());
            }
            let select_list_name = "SelectList".to_string();
            let select_list_format_ctx =
                AstFormatContext::with_children(select_list_name, select_list_children.len());
            let select_list_node =
                FormatTreeNode::with_children(select_list_format_ctx, select_list_children);
            children.push(select_list_node);
        }
        if !stmt.from.is_empty() {
            let mut table_list_children = Vec::with_capacity(stmt.from.len());
            for table in stmt.from.iter() {
                self.visit_table_reference(table);
                table_list_children.push(self.children.pop().unwrap());
            }
            let table_list_name = "TableList".to_string();
            let table_list_format_ctx =
                AstFormatContext::with_children(table_list_name, table_list_children.len());
            let table_list_node =
                FormatTreeNode::with_children(table_list_format_ctx, table_list_children);
            children.push(table_list_node);
        }
        if let Some(selection) = &stmt.selection {
            self.visit_expr(selection);
            let selection_child = self.children.pop().unwrap();
            let selection_name = "Where".to_string();
            let selection_format_ctx = AstFormatContext::with_children(selection_name, 1);
            let selection_node =
                FormatTreeNode::with_children(selection_format_ctx, vec![selection_child]);
            children.push(selection_node);
        }
        if !stmt.group_by.is_empty() {
            let mut group_by_list_children = Vec::with_capacity(stmt.group_by.len());
            for group_by in stmt.group_by.iter() {
                self.visit_expr(group_by);
                group_by_list_children.push(self.children.pop().unwrap());
            }
            let group_by_list_name = "GroupByList".to_string();
            let group_by_list_format_ctx =
                AstFormatContext::with_children(group_by_list_name, group_by_list_children.len());
            let group_by_list_node =
                FormatTreeNode::with_children(group_by_list_format_ctx, group_by_list_children);
            children.push(group_by_list_node);
        }
        if let Some(having) = &stmt.having {
            self.visit_expr(having);
            let having_child = self.children.pop().unwrap();
            let having_name = "Having".to_string();
            let having_format_ctx = AstFormatContext::with_children(having_name, 1);
            let having_node = FormatTreeNode::with_children(having_format_ctx, vec![having_child]);
            children.push(having_node);
        }

        let name = "SelectQuery".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }

    fn visit_select_target(&mut self, target: &'ast SelectTarget<'ast>) {
        match target {
            SelectTarget::AliasedExpr { expr, alias } => {
                self.visit_expr(expr);
                let child = self.children.pop().unwrap();
                let name = "Target".to_string();
                let format_ctx = if let Some(alias) = alias {
                    AstFormatContext::with_children_alias(name, 1, Some(format!("{}", alias)))
                } else {
                    AstFormatContext::with_children(name, 1)
                };
                let node = FormatTreeNode::with_children(format_ctx, vec![child]);
                self.children.push(node);
            }
            SelectTarget::QualifiedName(_) => {
                let name = format!("Target {}", target);
                let format_ctx = AstFormatContext::new(name);
                let node = FormatTreeNode::new(format_ctx);
                self.children.push(node);
            }
        }
    }

    fn visit_table_reference(&mut self, table: &'ast TableReference<'ast>) {
        match table {
            TableReference::Table {
                span: _,
                catalog,
                database,
                table,
                alias,
                travel_point,
            } => {
                let mut name = String::new();
                name.push_str("TableIdentifier ");
                if let Some(catalog) = catalog {
                    name.push_str(&catalog.to_string());
                    name.push('.');
                }
                if let Some(database) = database {
                    name.push_str(&database.to_string());
                    name.push('.');
                }
                name.push_str(&table.to_string());

                let mut children = Vec::new();
                if let Some(travel_point) = travel_point {
                    self.visit_time_travel_point(travel_point);
                    children.push(self.children.pop().unwrap());
                }
                let format_ctx = if let Some(alias) = alias {
                    AstFormatContext::with_children_alias(
                        name,
                        children.len(),
                        Some(format!("{}", alias)),
                    )
                } else {
                    AstFormatContext::with_children(name, children.len())
                };
                let node = FormatTreeNode::with_children(format_ctx, children);
                self.children.push(node);
            }
            TableReference::Subquery {
                span: _,
                subquery,
                alias,
            } => {
                self.visit_query(subquery);
                let child = self.children.pop().unwrap();
                let name = "Subquery".to_string();
                let format_ctx = if let Some(alias) = alias {
                    AstFormatContext::with_children_alias(name, 1, Some(format!("{}", alias)))
                } else {
                    AstFormatContext::with_children(name, 1)
                };
                let node = FormatTreeNode::with_children(format_ctx, vec![child]);
                self.children.push(node);
            }
            TableReference::TableFunction {
                span: _,
                name,
                params,
                alias,
            } => {
                let mut children = Vec::with_capacity(params.len());
                for param in params.iter() {
                    self.visit_expr(param);
                    children.push(self.children.pop().unwrap());
                }
                let func_name = format!("TableFunction {}", name);
                let format_ctx = if let Some(alias) = alias {
                    AstFormatContext::with_children_alias(
                        func_name,
                        children.len(),
                        Some(format!("{}", alias)),
                    )
                } else {
                    AstFormatContext::with_children(func_name, children.len())
                };
                let node = FormatTreeNode::with_children(format_ctx, children);
                self.children.push(node);
            }
            TableReference::Stage {
                span: _,
                name,
                path,
                files: _,
                alias,
            } => {
                // TODO do know what am I doing
                let stage_name = format!("Stage {}{}", name, path);
                let format_ctx = if let Some(alias) = alias {
                    AstFormatContext::with_children_alias(stage_name, 1, Some(format!("{}", alias)))
                } else {
                    AstFormatContext::with_children(stage_name, 0)
                };
                let node = FormatTreeNode::with_children(format_ctx, vec![]);
                self.children.push(node);
            }
            TableReference::Join { span: _, join } => {
                self.visit_join(join);
                let child = self.children.pop().unwrap();
                let name = "TableJoin".to_string();
                let format_ctx = AstFormatContext::with_children(name, 1);
                let node = FormatTreeNode::with_children(format_ctx, vec![child]);
                self.children.push(node);
            }
        }
    }

    fn visit_time_travel_point(&mut self, time: &'ast TimeTravelPoint<'ast>) {
        match time {
            TimeTravelPoint::Snapshot(sid) => {
                let name = format!("Snapshot {}", sid);
                let format_ctx = AstFormatContext::new(name);
                let node = FormatTreeNode::new(format_ctx);
                self.children.push(node);
            }
            TimeTravelPoint::Timestamp(expr) => {
                self.visit_expr(expr);
                let child = self.children.pop().unwrap();
                let name = "Timestamp".to_string();
                let format_ctx = AstFormatContext::with_children(name, 1);
                let node = FormatTreeNode::with_children(format_ctx, vec![child]);
                self.children.push(node);
            }
        }
    }

    fn visit_join(&mut self, join: &'ast Join<'ast>) {
        let mut children = Vec::new();
        self.visit_table_reference(&*join.left);
        children.push(self.children.pop().unwrap());
        self.visit_table_reference(&*join.right);
        children.push(self.children.pop().unwrap());

        match &join.condition {
            JoinCondition::On(expr) => {
                self.visit_expr(expr);
                let child = self.children.pop().unwrap();
                let condition_on_name = "ConditionOn".to_string();
                let condition_on_format_ctx = AstFormatContext::with_children(condition_on_name, 1);
                let condition_on_node =
                    FormatTreeNode::with_children(condition_on_format_ctx, vec![child]);
                children.push(condition_on_node);
            }
            JoinCondition::Using(idents) => {
                let mut using_children = Vec::with_capacity(idents.len());
                for ident in idents.iter() {
                    self.visit_identifier(ident);
                    using_children.push(self.children.pop().unwrap());
                }
                let condition_using_name = "ConditionUsing".to_string();
                let condition_using_format_ctx =
                    AstFormatContext::with_children(condition_using_name, using_children.len());
                let condition_using_node =
                    FormatTreeNode::with_children(condition_using_format_ctx, using_children);
                children.push(condition_using_node);
            }
            JoinCondition::Natural => {
                let condition_natural_name = "ConditionNatural".to_string();
                let condition_natural_format_ctx = AstFormatContext::new(condition_natural_name);
                let condition_natural_node = FormatTreeNode::new(condition_natural_format_ctx);
                children.push(condition_natural_node);
            }
            JoinCondition::None => {
                let condition_name = "Condition".to_string();
                let condition_format_ctx = AstFormatContext::new(condition_name);
                let condition_node = FormatTreeNode::new(condition_format_ctx);
                children.push(condition_node);
            }
        }

        let name = "Join".to_string();
        let format_ctx = AstFormatContext::with_children(name, children.len());
        let node = FormatTreeNode::with_children(format_ctx, children);
        self.children.push(node);
    }
}
