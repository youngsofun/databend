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

use common_base::tokio;
use common_exception::Result;
use databend_query::storages::system::SettingsTable;
use databend_query::storages::Table;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_settings_table() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    ctx.get_settings().set_max_threads(2)?;

    let table: Arc<dyn Table> = Arc::new(SettingsTable::create(1));
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 3);

    let expected = vec![
        "+------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------------------+",
        "| name                               | value   | description                                                                                                                                |",
        "+------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------------------+",
        "| flight_client_timeout              | 60      | Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds                                         |",
        "| max_block_size                     | 10000   | Maximum block size for reading                                                                                                             |",
        "| storage_occ_backoff_init_delay_ms  | 5       | The initial retry delay in millisecond. By default,  it is 5 ms.                                                                           |",
        "| max_threads                        | 2       | The maximum number of threads to execute the request. By default, it is determined automatically.                                          |",
        "| storage_read_buffer_size           | 1048576 | The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.                                                             |",
        "| storage_occ_backoff_max_delay_ms   | 20000   | The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds. |",
        "| storage_occ_backoff_max_elapsed_ms | 120000  | The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes.                    |",
        "| parallel_read_threads              | 1       | The maximum number of parallelism for reading data. By default, it is 1.                                                                   |",
        "+------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------------------+",


    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
