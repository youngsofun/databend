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
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::storage::StorageAzblobConfig;
use common_meta_app::storage::StorageFsConfig;
use common_meta_app::storage::StorageGcsConfig;
use common_meta_app::storage::StorageHttpConfig;
use common_meta_app::storage::StorageIpfsConfig;
use common_meta_app::storage::StorageObsConfig;
use common_meta_app::storage::StorageOssConfig;
use common_meta_app::storage::StorageParams;
use common_meta_app::storage::StorageS3Config;
use common_meta_app::storage::StorageWebhdfsConfig;
use common_meta_app::storage::STDIN_FD;
use common_meta_app::storage::STORAGE_GCS_DEFAULT_ENDPOINT;
use common_meta_app::storage::STORAGE_IPFS_DEFAULT_ENDPOINT;
use common_meta_app::storage::STORAGE_S3_DEFAULT_ENDPOINT;
use percent_encoding::percent_decode_str;
use url::Url;

use crate::ast::write_comma_separated_map;
use crate::ast::Scheme;

/// secure_omission will fix omitted endpoint url schemes into 'https://'
#[inline]
fn secure_omission(endpoint: String) -> String {
    // checking with starts_with() should be enough here
    if !endpoint.starts_with("https://") && !endpoint.starts_with("http://") {
        format!("https://{}", endpoint)
    } else {
        endpoint
    }
}

fn parse_azure_params(l: &mut UriLocationArgs, root: String) -> Result<StorageParams> {
    let endpoint = l.options.get("endpoint_url").cloned().ok_or_else(|| {
        ErrorCode::BadArguments(format!("endpoint_url is required for storage azblob"))
    })?;
    let sp = StorageParams::Azblob(StorageAzblobConfig {
        endpoint_url: secure_omission(endpoint),
        container: l.addr.to_string(),
        account_name: l.options.get("account_name").cloned().unwrap_or_default(),
        account_key: l.options.get("account_key").cloned().unwrap_or_default(),
        root,
    });

    l.options.check()?;

    Ok(sp)
}

fn parse_s3_params(l: &mut UriLocationArgs, root: String) -> Result<StorageParams> {
    let endpoint = l
        .options
        .get("endpoint_url")
        .cloned()
        .unwrap_or_else(|| STORAGE_S3_DEFAULT_ENDPOINT.to_string());

    // we split those field out to make borrow checker happy.
    let region = l.options.get("region").cloned().unwrap_or_default();

    let access_key_id = {
        if let Some(id) = l.options.get("access_key_id") {
            id
        } else if let Some(id) = l.options.get("aws_key_id") {
            id
        } else {
            ""
        }
    }
    .to_string();

    let secret_access_key = {
        if let Some(key) = l.options.get("secret_access_key") {
            key
        } else if let Some(key) = l.options.get("aws_secret_key") {
            key
        } else {
            ""
        }
    }
    .to_string();

    let security_token = {
        if let Some(token) = l.options.get("session_token") {
            token
        } else if let Some(token) = l.options.get("aws_token") {
            token
        } else if let Some(token) = l.options.get("security_token") {
            token
        } else {
            ""
        }
    }
    .to_string();

    let master_key = l.options.get("master_key").cloned().unwrap_or_default();

    let enable_virtual_host_style = {
        if let Some(s) = l.options.get("enable_virtual_host_style") {
            s
        } else {
            "false"
        }
    }
    .to_string()
    .parse()
    .map_err(|err| {
        ErrorCode::BadArguments(format!(
            "value for enable_virtual_host_style = (true|false) is invalid: {err:?}"
        ))
    })?;

    let role_arn = {
        if let Some(role) = l.options.get("role_arn") {
            role
        } else if let Some(role) = l.options.get("aws_role_arn") {
            role
        } else {
            ""
        }
    }
    .to_string();

    let external_id = {
        if let Some(id) = l.options.get("external_id") {
            id
        } else if let Some(id) = l.options.get("aws_external_id") {
            id
        } else {
            ""
        }
    }
    .to_string();

    let allow_anonymous = {
        if let Some(s) = l.options.get("allow_anonymous") {
            s
        } else {
            "false"
        }
    }
    .to_string()
    .parse()
    .map_err(|err| {
        ErrorCode::BadArguments(format!("value for allow_anonymous is invalid: {err:?}"))
    })?;

    let sp = StorageParams::S3(StorageS3Config {
        endpoint_url: secure_omission(endpoint),
        region,
        bucket: l.addr.to_string(),
        access_key_id,
        secret_access_key,
        security_token,
        master_key,
        root,
        // Disable credential load by default.
        // set to !allow_insecure when binding.
        disable_credential_loader: true,
        enable_virtual_host_style,
        role_arn,
        external_id,
        allow_anonymous,
    });

    l.options.check()?;

    Ok(sp)
}

fn parse_gcs_params(l: &mut UriLocationArgs) -> Result<StorageParams> {
    let endpoint = l
        .options
        .get("endpoint_url")
        .cloned()
        .unwrap_or_else(|| STORAGE_GCS_DEFAULT_ENDPOINT.to_string());
    let sp = StorageParams::Gcs(StorageGcsConfig {
        endpoint_url: secure_omission(endpoint),
        bucket: l.addr.clone(),
        root: l.path.clone(),
        credential: l.options.get("credential").cloned().unwrap_or_default(),
    });

    l.options.check()?;

    Ok(sp)
}

fn parse_ipfs_params(l: &mut UriLocationArgs) -> Result<StorageParams> {
    let endpoint = l
        .options
        .get("endpoint_url")
        .cloned()
        .unwrap_or_else(|| STORAGE_IPFS_DEFAULT_ENDPOINT.to_string());
    let sp = StorageParams::Ipfs(StorageIpfsConfig {
        endpoint_url: secure_omission(endpoint),
        root: "/ipfs/".to_string() + l.addr.as_str(),
    });

    l.options.check()?;

    Ok(sp)
}

fn parse_oss_params(l: &mut UriLocationArgs, root: String) -> Result<StorageParams> {
    let endpoint = l
        .options
        .get("endpoint_url")
        .cloned()
        .map(secure_omission)
        .ok_or_else(|| {
            ErrorCode::BadArguments(format!("endpoint_url is required for storage oss"))
        })?;
    let sp = StorageParams::Oss(StorageOssConfig {
        endpoint_url: endpoint,
        presign_endpoint_url: "".to_string(),
        bucket: l.addr.to_string(),
        access_key_id: l.options.get("access_key_id").cloned().unwrap_or_default(),
        access_key_secret: l
            .options
            .get("access_key_secret")
            .cloned()
            .unwrap_or_default(),
        root,
        // TODO(xuanwo): Support SSE in stage later.
        server_side_encryption: "".to_string(),
        server_side_encryption_key_id: "".to_string(),
    });

    l.options.check()?;

    Ok(sp)
}

fn parse_obs_params(l: &mut UriLocationArgs, root: String) -> Result<StorageParams> {
    let endpoint = l
        .options
        .get("endpoint_url")
        .cloned()
        .map(secure_omission)
        .ok_or_else(|| {
            ErrorCode::BadArguments(format!("endpoint_url is required for storage obs"))
        })?;
    let sp = StorageParams::Obs(StorageObsConfig {
        endpoint_url: endpoint,
        bucket: l.addr.to_string(),
        access_key_id: l.options.get("access_key_id").cloned().unwrap_or_default(),
        secret_access_key: l
            .options
            .get("secret_access_key")
            .cloned()
            .unwrap_or_default(),
        root,
    });

    l.options.check()?;

    Ok(sp)
}

/// following https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/HadoopIntegrationGuide/libhdfs/HdfsURL.htm
fn parse_hdfs_params(l: &mut UriLocationArgs) -> Result<StorageParams> {
    let name_node_from_uri = if l.addr.is_empty() {
        None
    } else {
        Some(format!("hdfs://{}", l.addr))
    };
    let name_node_option = l.options.get("name_node");

    let name_node = match (name_node_option, name_node_from_uri) {
        (Some(n1), Some(n2)) => {
            if n1 != &n2 {
                return Err(ErrorCode::BadArguments(format!(
                    "name_node in uri({n2}) and from option 'name_node' {n1} are not matched"
                )));
            } else {
                n2
            }
        }
        (Some(n1), None) => n1.to_string(),
        (None, Some(n2)) => n2,
        (None, None) => {
            // we prefer user to specify name_node in options
            return Err(ErrorCode::BadArguments(
                "name_node is required for storage hdfs",
            ));
        }
    };
    let sp = StorageParams::Hdfs(common_meta_app::storage::StorageHdfsConfig {
        name_node,
        root: l.path.clone(),
    });
    l.options.check()?;
    Ok(sp)
}

// The FileSystem scheme of WebHDFS is “webhdfs://”. A WebHDFS FileSystem URI has the following format.
// webhdfs://<HOST>:<HTTP_PORT>/<PATH>
fn parse_webhdfs_params(l: &mut UriLocationArgs) -> Result<StorageParams> {
    let is_https = l
        .options
        .get("https")
        .map(|s| s.to_lowercase().parse::<bool>())
        .unwrap_or(Ok(true))
        .map_err(|e| {
            ErrorCode::BadArguments(format!(
                "HTTPS should be `TRUE` or `FALSE`, parse error with: {:?}",
                e,
            ))
        })?;
    let prefix = if is_https { "https" } else { "http" };
    let endpoint_url = format!("{prefix}://{}", l.addr);

    let root = l.path.clone();

    let delegation = l.options.get("delegation").cloned().unwrap_or_default();

    let sp = StorageParams::Webhdfs(StorageWebhdfsConfig {
        endpoint_url,
        root,
        delegation,
    });

    l.options.check()?;

    Ok(sp)
}

fn parse_http_params(l: &mut UriLocationArgs) -> Result<StorageParams> {
    // Make sure path has been percent decoded before parse pattern.
    let path = percent_decode_str(&l.path).decode_utf8_lossy();
    let cfg = StorageHttpConfig {
        endpoint_url: format!("{}://{}", l.protocol, l.addr),
        paths: globiter::Pattern::parse(&path)
            .map_err(|err| {
                ErrorCode::BadArguments(format!("input path is not a valid glob: {err:?}"))
            })?
            .iter()
            .collect(),
    };
    Ok(StorageParams::Http(cfg))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionOptions {
    visited_keys: HashSet<String>,
    conns: BTreeMap<String, String>,
}

impl ConnectionOptions {
    pub fn new(conns: BTreeMap<String, String>) -> Self {
        Self {
            visited_keys: HashSet::new(),
            conns,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<&String> {
        self.visited_keys.insert(key.to_string());
        self.conns.get(key)
    }

    pub fn check(&self) -> Result<()> {
        let conn_keys = HashSet::from_iter(self.conns.keys().cloned());
        let diffs: Vec<String> = conn_keys
            .difference(&self.visited_keys)
            .map(|x| x.to_string())
            .collect();

        if !diffs.is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "connection params invalid: expected [{}], got [{}]",
                self.visited_keys
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(","),
                diffs.join(",")
            )));
        }
        Ok(())
    }
}

impl Display for ConnectionOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.conns.is_empty() {
            write!(f, " CONNECTION = ( ")?;
            write_comma_separated_map(f, &self.conns)?;
            write!(f, " )")?;
        }
        Ok(())
    }
}

pub struct UriLocationRaw {
    pub uri: String,
    pub connection_options: ConnectionOptions,
    pub part_prefix: String,
}

impl UriLocationRaw {
    pub fn new(
        uri: String,
        connection_options: BTreeMap<String, String>,
        part_prefix: String,
    ) -> Self {
        Self {
            uri,
            connection_options: ConnectionOptions::new(connection_options),
            part_prefix,
        }
    }
}

/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UriLocation {
    pub path: String,
    pub storage_params: StorageParams,

    // only used in create_share_endpoint
    pub url: String,
    // only used in create external table'
    pub part_prefix: String,
}

impl UriLocation {
    pub fn parse(
        uri: String,
        part_prefix: String,
        connection_options: BTreeMap<String, String>,
    ) -> Result<UriLocation> {
        let mut args = UriLocationArgs::from_uri_and_options(uri, part_prefix, connection_options)?;
        parse_uri_location(&mut args)
    }
}

struct UriLocationArgs {
    pub(crate) protocol: String,
    pub(crate) addr: String,
    pub(crate) path: String,
    pub(crate) options: ConnectionOptions,
    pub(crate) part_prefix: String,
}

impl UriLocationArgs {
    pub fn new(
        protocol: String,
        addr: String,
        path: String,
        part_prefix: String,
        conns: BTreeMap<String, String>,
    ) -> Self {
        Self {
            protocol,
            addr,
            path,
            part_prefix,
            options: ConnectionOptions::new(conns),
        }
    }
    pub fn from_uri_and_options(
        uri: String,
        part_prefix: String,
        connection_options: BTreeMap<String, String>,
    ) -> common_exception::Result<Self> {
        // fs location is not a valid url, let's check it in advance.
        if let Some(path) = uri.strip_prefix("fs://") {
            return Ok(Self::new(
                "fs".to_string(),
                "".to_string(),
                path.to_string(),
                part_prefix,
                BTreeMap::default(),
            ));
        }

        let parsed = Url::parse(&uri)
            .map_err(|e| common_exception::ErrorCode::BadArguments(format!("invalid uri {}", e)))?;
        let protocol = parsed.scheme().to_string();
        let addr = parsed
            .host_str()
            .map(|hostname| {
                if let Some(port) = parsed.port() {
                    format!("{}:{}", hostname, port)
                } else {
                    hostname.to_string()
                }
            })
            .ok_or(common_exception::ErrorCode::BadArguments("invalid uri"))?;

        let path = if parsed.path().is_empty() {
            "/".to_string()
        } else {
            parsed.path().to_string()
        };

        Ok(Self {
            protocol,
            addr,
            path,
            part_prefix,
            options: ConnectionOptions::new(connection_options),
        })
    }
}

/// parse_uri_location will parse given UriLocationRaw into StorageParams and Path.
fn parse_uri_location(l: &mut UriLocationArgs) -> Result<UriLocation> {
    // Path endswith `/` means it's a directory, otherwise it's a file.
    // If the path is a directory, we will use this path as root.
    // If the path is a file, we will use `/` as root (which is the default value)
    let (root, path) = if l.path.ends_with('/') {
        (l.path.clone(), "/".to_string())
    } else {
        ("/".to_string(), l.path.clone())
    };

    let protocol = l.protocol.parse::<Scheme>()?;

    let storage_params = match protocol {
        Scheme::Azblob => parse_azure_params(l, root)?,
        Scheme::Gcs => parse_gcs_params(l)?,
        Scheme::Hdfs => parse_hdfs_params(l)?,
        Scheme::Ipfs => parse_ipfs_params(l)?,
        Scheme::S3 => parse_s3_params(l, root)?,
        Scheme::Obs => parse_obs_params(l, root)?,
        Scheme::Oss => parse_oss_params(l, root)?,
        Scheme::Http => parse_http_params(l)?,
        Scheme::Webhdfs => parse_webhdfs_params(l)?,
        Scheme::Fs => {
            if root == "/" && path == STDIN_FD {
                StorageParams::Memory
            } else {
                let cfg = StorageFsConfig { root };
                StorageParams::Fs(cfg)
            }
        }
        Scheme::Memory => unreachable!(),
    };

    let path = match protocol {
        // HTTP is special that we don't support dir, always return / instead.
        Scheme::Http => "/".to_string(),
        _ => path,
    };

    let url = format!("{}://{}{}", l.protocol, l.addr, l.path);

    Ok(UriLocation {
        path,
        storage_params,
        url,
        part_prefix: l.part_prefix.clone(),
    })
}

impl Display for UriLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "'path: {}, storage_params: ({})",
            self.path, self.storage_params
        )
    }
}
