// Copyright 2023-, Edge & Node, GraphOps, and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use alloy_primitives::Address;
use alloy_sol_types::Eip712Domain;
use anyhow::anyhow;
use ethers_core::types::U256;
use eventuals::Eventual;
use log::error;
use sqlx::{types::BigDecimal, PgPool};
use std::{collections::HashMap, sync::Arc};
use tap_core::tap_manager::SignedReceipt;

use crate::prelude::Allocation;

#[derive(Clone)]
pub struct TapManager {
    indexer_allocations: Eventual<HashMap<Address, Allocation>>,
    escrow_accounts: Eventual<HashMap<Address, U256>>,
    pgpool: PgPool,
    domain_separator: Arc<Eip712Domain>,
}

impl TapManager {
    pub fn new(
        pgpool: PgPool,
        indexer_allocations: Eventual<HashMap<Address, Allocation>>,
        escrow_accounts: Eventual<HashMap<Address, U256>>,
        domain_separator: Eip712Domain,
    ) -> Self {
        Self {
            indexer_allocations,
            escrow_accounts,
            pgpool,
            domain_separator: Arc::new(domain_separator),
        }
    }

    /// Checks that the receipt refers to eligible allocation ID and TAP sender.
    ///
    /// If the receipt is valid, it is stored in the database.
    ///
    /// The rest of the TAP receipt checks are expected to be performed out-of-band by the receipt aggregate requester
    /// service.
    pub async fn verify_and_store_receipt(
        &self,
        receipt: SignedReceipt,
    ) -> Result<(), anyhow::Error> {
        let allocation_id = &receipt.message.allocation_id;
        if !self
            .indexer_allocations
            .value()
            .await
            .map(|allocations| allocations.contains_key(allocation_id))
            .unwrap_or(false)
        {
            return Err(anyhow!(
                "Receipt allocation ID `{}` is not eligible for this indexer",
                allocation_id
            ));
        }

        let receipt_signer = receipt
            .recover_signer(self.domain_separator.as_ref())
            .map_err(|e| {
                error!("Failed to recover receipt signer: {}", e);
                anyhow!(e)
            })?;
        if !self
            .escrow_accounts
            .value()
            .await
            .map(|accounts| {
                accounts
                    .get(&receipt_signer)
                    .map_or(false, |balance| balance > &U256::zero())
            })
            .unwrap_or(false)
        {
            return Err(anyhow!(
                "Receipt sender `{}` is not eligible for this indexer",
                receipt_signer
            ));
        }

        // TODO: consider doing this in another async task to avoid slowing down the paid query flow.
        sqlx::query!(
            r#"
                INSERT INTO scalar_tap_receipts (allocation_id, timestamp_ns, receipt)
                VALUES ($1, $2, $3)
            "#,
            format!("{:?}", allocation_id)
                .strip_prefix("0x")
                .unwrap()
                .to_owned(),
            BigDecimal::from(receipt.message.timestamp_ns),
            serde_json::to_value(receipt).map_err(|e| anyhow!(e))?
        )
        .execute(&self.pgpool)
        .await
        .map_err(|e| {
            error!("Failed to store receipt: {}", e);
            anyhow!(e)
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::prelude::{AllocationStatus, SubgraphDeployment};
    use alloy_primitives::Address;
    use alloy_sol_types::{eip712_domain, Eip712Domain};
    use ethers::signers::{coins_bip39::English, LocalWallet, MnemonicBuilder, Signer};
    use keccak_hash::H256;
    use sqlx::postgres::PgListener;

    use tap_core::tap_manager::SignedReceipt;
    use tap_core::{eip_712_signed_message::EIP712SignedMessage, tap_receipt::Receipt};
    use toolshed::thegraph::DeploymentId;

    use crate::test_vectors;

    use super::*;

    /// Fixture to generate a wallet and address
    pub fn keys() -> (LocalWallet, Address) {
        let wallet: LocalWallet = MnemonicBuilder::<English>::default()
            .phrase("abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about")
            .build()
            .unwrap();
        let address = wallet.address();

        (wallet, Address::from_slice(address.as_bytes()))
    }

    pub fn domain() -> Eip712Domain {
        eip712_domain! {
            name: "TAP",
            version: "1",
            chain_id: 1,
            verifying_contract: Address::from([0x11u8; 20]),
        }
    }

    /// Fixture to generate a signed receipt using the wallet from `keys()`
    /// and the given `query_id` and `value`
    pub async fn create_signed_receipt(
        allocation_id: Address,
        nonce: u64,
        timestamp_ns: u64,
        value: u128,
    ) -> SignedReceipt {
        let (wallet, _) = keys();

        EIP712SignedMessage::new(
            &domain(),
            Receipt {
                allocation_id,
                nonce,
                timestamp_ns,
                value,
            },
            &wallet,
        )
        .await
        .unwrap()
    }

    #[ignore]
    #[sqlx::test]
    async fn test_verify_and_store_receipt(pgpool: PgPool) {
        // Listen to pg_notify events
        let mut listener = PgListener::connect_with(&pgpool).await.unwrap();
        listener
            .listen("scalar_tap_receipt_notification")
            .await
            .unwrap();

        let allocation_id =
            Address::from_str("0xdeadbeefcafebabedeadbeefcafebabedeadbeef").unwrap();
        let domain = domain();
        let signed_receipt =
            create_signed_receipt(allocation_id, u64::MAX, u64::MAX, u128::MAX).await;

        // Mock allocation
        let allocation = Allocation {
            id: allocation_id,
            subgraph_deployment: SubgraphDeployment {
                id: DeploymentId::from_str("QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap(),
                denied_at: None,
            },
            status: AllocationStatus::Active,
            allocated_tokens: U256::zero(),
            closed_at_epoch: None,
            closed_at_epoch_start_block_hash: None,
            poi: None,
            previous_epoch_start_block_hash: None,
            created_at_block_hash: H256::zero().to_string(),
            created_at_epoch: 0,
            indexer: *test_vectors::INDEXER_ADDRESS,
            query_fee_rebates: None,
            query_fees_collected: None,
        };
        let indexer_allocations = Eventual::from_value(HashMap::from_iter(
            vec![(allocation_id, allocation)].into_iter(),
        ));

        // Mock escrow accounts
        let escrow_accounts = Eventual::from_value(HashMap::from_iter(vec![(
            *test_vectors::INDEXER_ADDRESS,
            U256::from(123),
        )]));

        let tap_manager =
            TapManager::new(pgpool.clone(), indexer_allocations, escrow_accounts, domain);

        tap_manager
            .verify_and_store_receipt(signed_receipt.clone())
            .await
            .unwrap();

        // Check that the receipt DB insertion was notified (PG NOTIFY, see migrations for more info)
        let notification = tokio::time::timeout(std::time::Duration::from_secs(1), listener.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(notification.channel(), "scalar_tap_receipt_notification");

        // Deserialize the notification payload (json)
        let notification_payload: serde_json::Value =
            serde_json::from_str(notification.payload()).unwrap();
        assert_eq!(
            // The allocation ID is stored as a hex string in the DB, without the 0x prefix nor checksum, so we parse it
            // into an Address and then back to a string to compare it with the expected value.
            Address::from_str(notification_payload["allocation_id"].as_str().unwrap())
                .unwrap()
                .to_string(),
            allocation_id.to_string()
        );
        assert_eq!(notification_payload["timestamp_ns"], u64::MAX);
        assert!(notification_payload["id"].is_u64());
    }
}
