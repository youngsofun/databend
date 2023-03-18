// Copyright 2023 Datafuse Labs.
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

// The servers module used for external communication with user, such as MySQL wired protocol, etc.

use std::fs;

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::FlightData;
use common_base::base::tokio;
use databend_query::servers::flight_sql::flight2to1::flight_datas_to_data_block;
use databend_query::servers::flight_sql::flight_sql_server::FlightSqlServiceImpl;
use futures::TryStreamExt;
use tempfile::NamedTempFile;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Endpoint;
use tonic::transport::Server;
use tower::service_fn;

async fn client_with_uds(path: String) -> FlightSqlServiceClient {
    let connector = service_fn(move |_| UnixStream::connect(path.clone()));
    let channel = Endpoint::try_from("http://example.com")
        .unwrap()
        .connect_with_connector(connector)
        .await
        .unwrap();
    FlightSqlServiceClient::new(channel)
}

#[tokio::test]
async fn test_select_1() {
    let file = NamedTempFile::new().unwrap();
    let path = file.into_temp_path().to_str().unwrap().to_string();
    let _ = fs::remove_file(path.clone());

    let uds = UnixListener::bind(path.clone()).unwrap();
    let stream = UnixListenerStream::new(uds);

    // We would just listen on TCP, but it seems impossible to know when tonic is ready to serve
    let service = FlightSqlServiceImpl {};
    let serve_future = Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve_with_incoming(stream);

    let request_future = async {
        let mut client = client_with_uds(path).await;
        let token = client.handshake("admin", "password").await.unwrap();
        println!("Auth succeeded with token: {:?}", token);
        let mut stmt = client.prepare("select 1;".to_string()).await.unwrap();
        let flight_info = stmt.execute().await.unwrap();
        let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
        let flight_data = client.do_get(ticket).await.unwrap();
        let flight_data: Vec<FlightData> = flight_data.try_collect().await.unwrap();
        let (_schema, block) = flight_datas_to_data_block(flight_data).unwrap();
        let expected = r#"
        +-------------------+
        | salutation        |
        +-------------------+
        | Hello, FlightSQL! |
        +-------------------+"#
            .trim()
            .to_string();
        assert_eq!(block.to_string(), expected);
    };

    tokio::select! {
        _ = serve_future => panic!("server returned first"),
        _ = request_future => println!("Client finished!"),
    }
}
