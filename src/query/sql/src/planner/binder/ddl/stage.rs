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

use std::collections::BTreeMap;
use std::str::FromStr;

use common_ast::ast::CreateStageStmt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::FileFormatOptionsAst;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::StageInfo;
use common_storage::init_operator;

use super::super::copy_into_table::resolve_stage_location;
use crate::binder::location::bind_uri_location;
use crate::binder::Binder;
use crate::plans::CreateStagePlan;
use crate::plans::Plan;
use crate::plans::RemoveStagePlan;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_remove_stage(
        &mut self,
        location: &str,
        pattern: &str,
    ) -> Result<Plan> {
        let (stage, path) = resolve_stage_location(&self.ctx, location).await?;
        let plan_node = RemoveStagePlan {
            path,
            stage,
            pattern: pattern.to_string(),
        };

        Ok(Plan::RemoveStage(Box::new(plan_node)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_stage(
        &mut self,
        stmt: &CreateStageStmt,
    ) -> Result<Plan> {
        let CreateStageStmt {
            if_not_exists,
            stage_name,
            location,
            file_format_options,
            on_error,
            size_limit,
            validation_mode: _,
            comments: _,
        } = stmt;

        let mut stage_info = match location {
            None => {
                if stage_name == "~" {
                    StageInfo::new_user_stage(&self.ctx.get_current_user()?.name)
                } else {
                    StageInfo::new_internal_stage(stage_name)
                }
            }
            Some(uri) => {
                let stage_storage = bind_uri_location(&uri.storage_params).await?;

                if !uri.path.ends_with('/') {
                    return Err(ErrorCode::SyntaxException(
                        "URL's path must ends with `/` when do CREATE STAGE",
                    ));
                }

                // Check the storage params via init operator.
                let _ = init_operator(&stage_storage).map_err(|err| {
                    ErrorCode::InvalidConfig(format!(
                        "Input storage config for stage is not invalid: {err:?}"
                    ))
                })?;

                StageInfo::new_external_stage(stage_storage, &uri.path).with_stage_name(stage_name)
            }
        };

        if !file_format_options.is_empty() {
            stage_info.file_format_params =
                self.try_resolve_file_format(file_format_options).await?;
        }
        // Copy options.
        {
            // on_error.
            if !on_error.is_empty() {
                stage_info.copy_options.on_error =
                    OnErrorMode::from_str(on_error).map_err(ErrorCode::SyntaxException)?;
            }

            stage_info.copy_options.size_limit = *size_limit;
        }

        Ok(Plan::CreateStage(Box::new(CreateStagePlan {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            stage_info,
        })))
    }

    #[async_backtrace::framed]
    pub(crate) async fn try_resolve_file_format(
        &self,
        options: &BTreeMap<String, String>,
    ) -> Result<FileFormatParams> {
        if let Some(name) = options.get("format_name") {
            self.ctx.get_file_format(name).await
        } else {
            FileFormatParams::try_from(FileFormatOptionsAst {
                options: options.clone(),
            })
        }
    }
}
