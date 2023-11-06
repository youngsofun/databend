// Copyright 2021 Datafuse Labs
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
use std::str::FromStr;
use std::sync::Arc;

use common_ast::ast::ColumnID as AstColumnID;
use common_ast::ast::CopyIntoTableSource;
use common_ast::ast::CopyIntoTableStmt;
use common_ast::ast::Expr;
use common_ast::ast::FileLocation;
use common_ast::ast::Identifier;
use common_ast::ast::Query;
use common_ast::ast::SelectTarget;
use common_ast::ast::SetExpr;
use common_ast::ast::TableAlias;
use common_ast::ast::TableReference;
use common_ast::ast::TypeName;
use common_ast::parser::parser_values_with_placeholder;
use common_ast::parser::tokenize_sql;
use common_ast::Visitor;
use common_catalog::plan::StageTableInfo;
use common_catalog::table_context::StageAttachment;
use common_catalog::table_context::TableContext;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Evaluator;
use common_expression::Scalar;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::principal::FileFormatOptionsAst;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::StageInfo;
use common_storage::StageFilesInfo;
use common_users::UserApiProvider;
use indexmap::IndexMap;
use log::debug;
use parking_lot::RwLock;

use crate::binder::location::bind_uri_location;
use crate::binder::select::MaxColumnPosition;
use crate::binder::Binder;
use crate::plans::CopyIntoTableMode;
use crate::plans::CopyIntoTablePlan;
use crate::plans::Plan;
use crate::plans::ValidationMode;
use crate::BindContext;
use crate::Metadata;
use crate::NameResolutionContext;
use crate::ScalarBinder;

impl<'a> Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_copy_into_table(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CopyIntoTableStmt,
    ) -> Result<Plan> {
        match &stmt.src {
            CopyIntoTableSource::Location(location) => {
                let plan = self
                    .bind_copy_into_table_common(bind_context, stmt, location)
                    .await?;
                self.bind_copy_into_table_from_location(bind_context, plan)
                    .await
            }
            CopyIntoTableSource::Query(query) => {
                let mut max_column_position = MaxColumnPosition::new();
                max_column_position.visit_query(query.as_ref());
                self.metadata
                    .write()
                    .set_max_column_position(max_column_position.max_pos);
                let (select_list, location, alias) = check_transform_query(query)?;
                let plan = self
                    .bind_copy_into_table_common(bind_context, stmt, location)
                    .await?;
                self.bind_copy_from_query_into_table(bind_context, plan, select_list, alias)
                    .await
            }
        }
    }
    async fn bind_copy_into_table_common(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CopyIntoTableStmt,
        location: &FileLocation,
    ) -> Result<CopyIntoTablePlan> {
        let (catalog_name, database_name, table_name) = self.normalize_object_identifier_triple(
            &stmt.dst.catalog,
            &stmt.dst.database,
            &stmt.dst.table,
        );
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        let catalog_info = catalog.info();
        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;

        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (mut stage_info, path) = resolve_file_location(&self.ctx, location).await?;
        self.apply_copy_into_table_options(stmt, &mut stage_info)
            .await?;
        let files_info = StageFilesInfo {
            path,
            files: stmt.files.clone(),
            pattern: stmt.pattern.clone(),
        };
        let required_values_schema: DataSchemaRef = Arc::new(
            match &stmt.dst_columns {
                Some(cols) => self.schema_project(&table.schema(), cols)?,
                None => self.schema_project(&table.schema(), &[])?,
            }
            .into(),
        );

        let stage_schema = infer_table_schema(&required_values_schema)?;
        let default_values = self
            .prepare_default_values(bind_context, &required_values_schema)
            .await?;

        Ok(CopyIntoTablePlan {
            catalog_info,
            database_name,
            table_name,
            validation_mode,
            no_file_to_copy: false,
            from_attachment: false,
            force: stmt.force,
            stage_table_info: StageTableInfo {
                schema: stage_schema,
                files_info,
                stage_info,
                files_to_copy: None,
                is_select: false,
                default_values: Some(default_values),
            },
            values_consts: vec![],
            required_source_schema: required_values_schema.clone(),
            required_values_schema: required_values_schema.clone(),
            write_mode: CopyIntoTableMode::Copy,
            query: None,

            enable_distributed: false,
        })
    }

    /// Bind COPY INFO <table> FROM <stage_location>
    #[async_backtrace::framed]
    async fn bind_copy_into_table_from_location(
        &mut self,
        bind_ctx: &BindContext,
        plan: CopyIntoTablePlan,
    ) -> Result<Plan> {
        if matches!(
            plan.stage_table_info.stage_info.file_format_params,
            FileFormatParams::Parquet(_)
        ) {
            let select_list = plan
                .required_source_schema
                .fields()
                .iter()
                .map(|f| {
                    let column = Expr::ColumnRef {
                        span: None,
                        database: None,
                        table: None,
                        column: AstColumnID::Name(Identifier::from_name(f.name().to_string())),
                    };
                    let expr = if f.data_type().remove_nullable() == DataType::Variant {
                        Expr::Cast {
                            span: None,
                            expr: Box::new(column),
                            target_type: TypeName::Variant,
                            pg_style: false,
                        }
                    } else {
                        column
                    };
                    SelectTarget::AliasedExpr {
                        expr: Box::new(expr),
                        alias: None,
                    }
                })
                .collect::<Vec<_>>();

            self.bind_copy_from_query_into_table(bind_ctx, plan, &select_list, &None)
                .await
        } else {
            Ok(Plan::CopyIntoTable(Box::new(plan)))
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_attachment(
        &mut self,
        attachment: StageAttachment,
    ) -> Result<(StageInfo, StageFilesInfo)> {
        let (mut stage_info, path) =
            resolve_stage_location(&self.ctx, &attachment.location[1..]).await?;

        if let Some(ref options) = attachment.file_format_options {
            stage_info.file_format_params = FileFormatOptionsAst {
                options: options.clone(),
            }
            .try_into()?;
        }
        if let Some(ref options) = attachment.copy_options {
            stage_info.copy_options.apply(options, true)?;
        }

        let files_info = StageFilesInfo {
            path,
            files: None,
            pattern: None,
        };
        Ok((stage_info, files_info))
    }

    /// Bind COPY INFO <table> FROM <location>
    /// called by bind_insert
    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub(crate) async fn bind_copy_from_attachment(
        &mut self,
        bind_context: &mut BindContext,
        attachment: StageAttachment,
        catalog_name: String,
        database_name: String,
        table_name: String,
        required_values_schema: DataSchemaRef,
        values_str: &str,
        write_mode: CopyIntoTableMode,
    ) -> Result<Plan> {
        let (data_schema, const_columns) = if values_str.is_empty() {
            (required_values_schema.clone(), vec![])
        } else {
            self.prepared_values(values_str, &required_values_schema)
                .await?
        };

        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        let catalog_info = catalog.info();

        let (stage_info, files_info) = self.bind_attachment(attachment).await?;

        let stage_schema = infer_table_schema(&data_schema)?;

        let default_values = self
            .prepare_default_values(bind_context, &data_schema)
            .await?;

        let plan = CopyIntoTablePlan {
            catalog_info,
            database_name,
            table_name,
            no_file_to_copy: false,
            from_attachment: true,
            required_source_schema: data_schema.clone(),
            required_values_schema,
            values_consts: const_columns,
            force: true,
            stage_table_info: StageTableInfo {
                schema: stage_schema,
                files_info,
                stage_info,
                files_to_copy: None,
                is_select: false,
                default_values: Some(default_values),
            },
            write_mode,
            query: None,
            validation_mode: ValidationMode::None,

            enable_distributed: false,
        };

        self.bind_copy_into_table_from_location(bind_context, plan)
            .await
    }

    /// Bind COPY INTO <table> FROM <query>
    #[async_backtrace::framed]
    async fn bind_copy_from_query_into_table(
        &mut self,
        bind_context: &BindContext,
        mut plan: CopyIntoTablePlan,
        select_list: &'a [SelectTarget],
        alias: &Option<TableAlias>,
    ) -> Result<Plan> {
        let need_copy_file_infos = plan.collect_files(self.ctx.as_ref()).await?;

        if need_copy_file_infos.is_empty() {
            plan.no_file_to_copy = true;
            return Ok(Plan::CopyIntoTable(Box::new(plan)));
        }
        plan.stage_table_info.files_to_copy = Some(need_copy_file_infos.clone());

        let table_ctx = self.ctx.clone();
        let (s_expr, mut from_context) = self
            .bind_stage_table(
                table_ctx,
                bind_context,
                plan.stage_table_info.stage_info.clone(),
                plan.stage_table_info.files_info.clone(),
                alias,
                Some(need_copy_file_infos.clone()),
            )
            .await?;

        // Generate a analyzed select list with from context
        let select_list = self
            .normalize_select_list(&mut from_context, select_list)
            .await?;
        let (scalar_items, projections) =
            self.analyze_projection(&from_context.aggregate_info, &select_list)?;

        if projections.len() != plan.required_source_schema.num_fields() {
            return Err(ErrorCode::BadArguments(format!(
                "Number of columns in select list ({}) does not match that of the corresponding table ({})",
                projections.len(),
                plan.required_source_schema.num_fields(),
            )));
        }

        let s_expr =
            self.bind_projection(&mut from_context, &projections, &scalar_items, s_expr)?;
        let mut output_context = BindContext::new();
        output_context.parent = from_context.parent;
        output_context.columns = from_context.columns;

        plan.query = Some(Box::new(Plan::Query {
            s_expr: Box::new(s_expr),
            metadata: self.metadata.clone(),
            bind_context: Box::new(output_context),
            rewrite_kind: None,
            ignore_result: false,
            formatted_ast: None,
        }));
        Ok(Plan::CopyIntoTable(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub async fn apply_copy_into_table_options(
        &mut self,
        stmt: &CopyIntoTableStmt,
        stage: &mut StageInfo,
    ) -> Result<()> {
        if !stmt.file_format.is_empty() {
            stage.file_format_params = self.try_resolve_file_format(&stmt.file_format).await?;
        }

        // Copy options.
        {
            // on_error.
            stage.copy_options.on_error =
                OnErrorMode::from_str(&stmt.on_error).map_err(ErrorCode::SyntaxException)?;

            // size_limit.
            if stmt.size_limit != 0 {
                stage.copy_options.size_limit = stmt.size_limit;
            }
            if stmt.max_files != 0 {
                stage.copy_options.max_files = stmt.max_files;
            }
            stage.copy_options.split_size = stmt.split_size;
            stage.copy_options.purge = stmt.purge;
            stage.copy_options.disable_variant_check = stmt.disable_variant_check;
            stage.copy_options.return_failed_only = stmt.return_failed_only;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub(crate) async fn prepared_values(
        &self,
        values_str: &str,
        source_schema: &DataSchemaRef,
    ) -> Result<(DataSchemaRef, Vec<Scalar>)> {
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let tokens = tokenize_sql(values_str)?;
        let expr_or_placeholders = parser_values_with_placeholder(&tokens, sql_dialect)?;

        if source_schema.num_fields() != expr_or_placeholders.len() {
            return Err(ErrorCode::SemanticError(format!(
                "need {} fields in values, got only {}",
                source_schema.num_fields(),
                expr_or_placeholders.len()
            )));
        }

        let mut attachment_fields = vec![];
        let mut const_fields = vec![];
        let mut exprs = vec![];
        for (i, eo) in expr_or_placeholders.into_iter().enumerate() {
            match eo {
                Some(e) => {
                    exprs.push(e);
                    const_fields.push(source_schema.fields()[i].clone());
                }
                None => attachment_fields.push(source_schema.fields()[i].clone()),
            }
        }
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let mut bind_context = BindContext::new();
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let const_schema = Arc::new(DataSchema::new(const_fields));
        let const_values = bind_context
            .exprs_to_scalar(
                exprs,
                &const_schema,
                self.ctx.clone(),
                &name_resolution_ctx,
                metadata,
            )
            .await?;
        Ok((Arc::new(DataSchema::new(attachment_fields)), const_values))
    }

    async fn prepare_default_values(
        &mut self,
        bind_context: &mut BindContext,
        data_schema: &DataSchemaRef,
    ) -> Result<Vec<Scalar>> {
        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            HashMap::new(),
            Box::new(IndexMap::new()),
        );
        let func_ctx = self.ctx.get_function_context()?;
        let input = DataBlock::empty();
        let evaluator = Evaluator::new(&input, &func_ctx, &BUILTIN_FUNCTIONS);

        let mut values = vec![];
        for field in &data_schema.fields {
            let expr = scalar_binder.get_default_value(field, data_schema).await?;
            values.push(evaluator.run(&expr)?.as_scalar().unwrap().clone());
        }
        Ok(values)
    }
}

// we can avoid this by specializing the parser.
// make parse a little more complex, now it is COPY ~ INTO ~ #copy_unit ~ FROM ~ #copy_unit
// also check_query here may give a more friendly error msg.
fn check_transform_query(
    query: &Query,
) -> Result<(&Vec<SelectTarget>, &FileLocation, &Option<TableAlias>)> {
    if query.offset.is_none()
        && query.limit.is_empty()
        && query.order_by.is_empty()
        && query.with.is_none()
    {
        if let SetExpr::Select(select) = &query.body {
            if select.group_by.is_none()
                && !select.distinct
                && select.having.is_none()
                && select.from.len() == 1
            {
                if let TableReference::Location {
                    span: _,
                    location,
                    options,
                    alias,
                } = &select.from[0]
                {
                    if options.is_empty() {
                        return Ok((&select.select_list, location, alias));
                    } else {
                        return Err(ErrorCode::SyntaxException(
                            "stage table function inside copy not allow options, apply them in the outer copy stmt instead.",
                        ));
                    }
                }
            }
        }
    }
    Err(ErrorCode::SyntaxException(
        "query as source of copy only allow projection on one stage table",
    ))
}

/// Named stage(start with `@`):
///
/// ```sql
/// copy into mytable from @my_ext_stage
///     file_format = (type = csv);
/// ```
///
/// Returns user's stage info and relative path towards the stage's root.
///
/// If input location is empty we will convert it to `/` means the root of stage
///
/// - @mystage => (mystage, "/")
///
/// If input location is endswith `/`, it's a folder.
///
/// - @mystage/ => (mystage, "/")
///
/// Otherwise, it's a file
///
/// - @mystage/abc => (mystage, "abc")
///
/// For internal stage, we will also add prefix `/stage/<stage>/`
///
/// - @internal/abc => (internal, "/stage/internal/abc")
#[async_backtrace::framed]
pub async fn resolve_stage_location(
    ctx: &Arc<dyn TableContext>,
    location: &str,
) -> Result<(StageInfo, String)> {
    // my_named_stage/abc/
    let names: Vec<&str> = location.splitn(2, '/').filter(|v| !v.is_empty()).collect();

    let stage = if names[0] == "~" {
        StageInfo::new_user_stage(&ctx.get_current_user()?.name)
    } else {
        UserApiProvider::instance()
            .get_stage(&ctx.get_tenant(), names[0])
            .await?
    };

    let path = names.get(1).unwrap_or(&"").trim_start_matches('/');
    let path = if path.is_empty() { "/" } else { path };

    debug!("parsed stage: {stage:?}, path: {path}");
    Ok((stage, path.to_string()))
}

#[async_backtrace::framed]
pub async fn resolve_file_location(
    ctx: &Arc<dyn TableContext>,
    location: &FileLocation,
) -> Result<(StageInfo, String)> {
    match location.clone() {
        FileLocation::Stage(location) => resolve_stage_location(ctx, &location).await,
        FileLocation::Uri(uri) => {
            let path = uri.path.clone();
            let storage_params = bind_uri_location(&uri.storage_params).await?;
            if !storage_params.is_secure() && !GlobalConfig::instance().storage.allow_insecure {
                Err(ErrorCode::StorageInsecure(
                    "copy from insecure storage is not allowed",
                ))
            } else {
                let stage_info = StageInfo::new_external_stage(storage_params, &path);
                Ok((stage_info, path))
            }
        }
    }
}
