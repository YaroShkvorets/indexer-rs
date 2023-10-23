use std::net::SocketAddr;

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatabaseConfig {
    pub postgres_url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NetworkSubgraphConfig {
    pub query_url: String,
    pub syncing_interval: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EscrowSubgraphConfig {
    pub query_url: String,
    pub syncing_interval: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host_and_port: SocketAddr,
    pub url_prefix: String,
    pub free_query_auth_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IndexerServiceConfig {
    pub indexer: IndexerConfig,
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub network_subgraph: NetworkSubgraphConfig,
    pub escrow_subgraph: EscrowSubgraphConfig,
    pub graph_network: GraphNetworkConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GraphNetworkConfig {
    pub id: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IndexerConfig {
    pub indexer_address: Address,
    pub operator_mnemonic: String,
}
