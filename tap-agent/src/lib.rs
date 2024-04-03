use alloy_sol_types::{eip712_domain, Eip712Domain};
use lazy_static::lazy_static;

use crate::config::Cli;

lazy_static! {
    pub static ref CONFIG: Cli = Cli::args();
    pub static ref EIP_712_DOMAIN: Eip712Domain = eip712_domain! {
        name: "TAP",
        version: "1",
        chain_id: CONFIG.receipts.receipts_verifier_chain_id,
        verifying_contract: CONFIG.receipts.receipts_verifier_address,
    };
}

pub mod agent;
pub mod aggregator_endpoints;
pub mod config;
pub mod database;
pub mod tap;