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

use std::collections::HashMap;
use std::sync::Arc;

pub use common_catalog::catalog::StorageDescription;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_storages_stage::StageTable;
use common_storages_stage::ENGINE_STAGE;
use parking_lot::RwLock;

use super::random::RandomTable;
use crate::storages::fuse::FuseTable;
use crate::storages::memory::MemoryTable;
use crate::storages::null::NullTable;
use crate::storages::view::ViewTable;
use crate::storages::StorageContext;
use crate::storages::Table;
use crate::Config;

pub trait StorageCreator: Send + Sync {
    fn try_create(&self, ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>>;
}

impl<T> StorageCreator for T
where
    T: Fn(StorageContext, TableInfo) -> Result<Box<dyn Table>>,
    T: Send + Sync,
{
    fn try_create(&self, ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        self(ctx, table_info)
    }
}

pub trait StorageDescriptor: Send + Sync {
    fn description(&self) -> StorageDescription;
}

impl<T> StorageDescriptor for T
where
    T: Fn() -> StorageDescription,
    T: Send + Sync,
{
    fn description(&self) -> StorageDescription {
        self()
    }
}

pub struct Storage {
    creator: Arc<dyn StorageCreator>,
    descriptor: Arc<dyn StorageDescriptor>,
}

#[derive(Default)]
pub struct StorageFactory {
    storages: RwLock<HashMap<String, Storage>>,
}

impl StorageFactory {
    pub fn create(conf: Config) -> Self {
        let mut creators: HashMap<String, Storage> = Default::default();

        // Register memory table engine.
        if conf.query.table_engine_memory_enabled {
            creators.insert("MEMORY".to_string(), Storage {
                creator: Arc::new(MemoryTable::try_create),
                descriptor: Arc::new(MemoryTable::description),
            });
        }

        // Register NULL table engine.
        creators.insert("NULL".to_string(), Storage {
            creator: Arc::new(NullTable::try_create),
            descriptor: Arc::new(NullTable::description),
        });

        // Register FUSE table engine.
        creators.insert("FUSE".to_string(), Storage {
            creator: Arc::new(FuseTable::try_create),
            descriptor: Arc::new(FuseTable::description),
        });

        // Register View table engine
        creators.insert("VIEW".to_string(), Storage {
            creator: Arc::new(ViewTable::try_create),
            descriptor: Arc::new(ViewTable::description),
        });

        // Register RANDOM table engine
        creators.insert("RANDOM".to_string(), Storage {
            creator: Arc::new(RandomTable::try_create),
            descriptor: Arc::new(RandomTable::description),
        });

        // Register STORAGE engine
        creators.insert(ENGINE_STAGE.to_string(), Storage {
            creator: Arc::new(StageTable::try_create),
            descriptor: Arc::new(StageTable::description),
        });
        StorageFactory {
            storages: RwLock::new(creators),
        }
    }

    pub fn get_table(&self, ctx: StorageContext, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let engine = table_info.engine().to_uppercase();
        let lock = self.storages.read();
        let factory = lock.get(&engine).ok_or_else(|| {
            ErrorCode::UnknownTableEngine(format!("Unknown table engine {}", engine))
        })?;

        let table: Arc<dyn Table> = factory.creator.try_create(ctx, table_info.clone())?.into();
        Ok(table)
    }

    pub fn get_storage_descriptors(&self) -> Vec<StorageDescription> {
        let lock = self.storages.read();
        let mut descriptors = Vec::with_capacity(lock.len());
        for value in lock.values() {
            descriptors.push(value.descriptor.description())
        }
        descriptors
    }
}
