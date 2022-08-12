// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_base::base::tokio::runtime::Runtime;
use common_base::base::Thread;
use common_exception::Result;
use databend_query::clusters::ClusterDiscovery;
use databend_query::sessions::SessionManager;
use databend_query::Config;

async fn async_create_sessions(config: Config) -> Result<Arc<SessionManager>> {
    ClusterDiscovery::init(config.clone()).await?;
    SessionManager::init(config.clone())?;

    let cluster_discovery = ClusterDiscovery::instance();
    cluster_discovery.register_to_metastore(&config).await?;
    Ok(SessionManager::instance())
}

fn sync_create_sessions(config: Config) -> Result<Arc<SessionManager>> {
    let runtime = Runtime::new()?;
    runtime.block_on(async_create_sessions(config))
}

pub struct SessionManagerBuilder {
    config: Config,
}

#[allow(dead_code)]
impl SessionManagerBuilder {
    pub fn create() -> SessionManagerBuilder {
        let conf = crate::tests::ConfigBuilder::create().config();

        SessionManagerBuilder::create_with_conf(conf)
    }

    pub fn create_with_conf(config: Config) -> SessionManagerBuilder {
        SessionManagerBuilder { config }
    }

    pub fn max_sessions(self, max_sessions: u64) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.max_active_sessions = max_sessions;
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn rpc_tls_server_key(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.rpc_tls_server_key = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn rpc_tls_server_cert(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.rpc_tls_server_cert = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn jwt_key_file(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.jwt_key_file = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn http_handler_result_time_out(self, value: impl Into<u64>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.http_handler_result_timeout_millis = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn http_handler_tls_server_key(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.http_handler_tls_server_key = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn http_handler_tls_server_cert(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.http_handler_tls_server_cert = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn http_handler_tls_server_root_ca_cert(
        self,
        value: impl Into<String>,
    ) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.http_handler_tls_server_root_ca_cert = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn api_tls_server_key(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.api_tls_server_key = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn api_tls_server_cert(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.api_tls_server_cert = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn api_tls_server_root_ca_cert(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.api_tls_server_root_ca_cert = value.into();
        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn build(self) -> Result<Arc<SessionManager>> {
        let config = self.config;
        let handle = Thread::spawn(move || sync_create_sessions(config));
        handle.join().unwrap()
    }
}
