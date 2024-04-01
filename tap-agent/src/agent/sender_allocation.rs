// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use alloy_primitives::hex::ToHex;
use alloy_sol_types::Eip712Domain;
use anyhow::{anyhow, ensure, Result};
use bigdecimal::num_bigint::BigInt;
use eventuals::Eventual;
use indexer_common::{escrow_accounts::EscrowAccounts, prelude::SubgraphClient};
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use sqlx::{types::BigDecimal, PgPool};
use tap_aggregator::jsonrpsee_helpers::JsonRpcResponse;
use tap_core::{
    rav::{RAVRequest, ReceiptAggregateVoucher},
    receipt::{
        checks::{Check, Checks},
        Failed, ReceiptWithState,
    },
    signed_message::EIP712SignedMessage,
};
use thegraph::types::Address;
use tracing::{error, warn};

use crate::agent::sender_account::SenderAccountMessage;
use crate::agent::sender_accounts_manager::NewReceiptNotification;
use crate::agent::unaggregated_receipts::UnaggregatedReceipts;
use crate::{
    config::{self},
    tap::context::{checks::Signature, TapAgentContext},
    tap::signers_trimmed,
    tap::{context::checks::AllocationId, escrow_adapter::EscrowAdapter},
};

type TapManager = tap_core::manager::Manager<TapAgentContext>;

/// Manages unaggregated fees and the TAP lifecyle for a specific (allocation, sender) pair.
pub struct SenderAllocation {
    pgpool: PgPool,
    tap_manager: TapManager,
    allocation_id: Address,
    sender: Address,
    sender_aggregator_endpoint: String,
    config: &'static config::Cli,
    escrow_accounts: Eventual<EscrowAccounts>,
    tap_eip712_domain_separator: Eip712Domain,
    sender_account_ref: ActorRef<SenderAccountMessage>,
}

pub enum SenderAllocationMessage {
    NewReceipt(NewReceiptNotification),
    TriggerRAVRequest(RpcReplyPort<UnaggregatedReceipts>),
    CloseAllocation,

    #[cfg(test)]
    GetUnaggregatedReceipts(RpcReplyPort<UnaggregatedReceipts>),
}

#[async_trait::async_trait]
impl Actor for SenderAllocation {
    type Msg = SenderAllocationMessage;
    type State = UnaggregatedReceipts;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> std::result::Result<Self::State, ActorProcessingErr> {
        let unaggregated_fees = self.calculate_unaggregated_fee().await?;
        self.sender_account_ref
            .cast(SenderAccountMessage::UpdateReceiptFees(
                self.allocation_id,
                unaggregated_fees.clone(),
            ))?;

        Ok(unaggregated_fees)
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        // cleanup receipt fees for sender
        self.sender_account_ref
            .cast(SenderAccountMessage::UpdateReceiptFees(
                self.allocation_id,
                UnaggregatedReceipts::default(),
            ))?;
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        match message {
            SenderAllocationMessage::NewReceipt(NewReceiptNotification {
                id, value: fees, ..
            }) => {
                if id > state.last_id {
                    state.last_id = id;
                    state.value = state.value.checked_add(fees).unwrap_or_else(|| {
                        // This should never happen, but if it does, we want to know about it.
                        error!(
                            "Overflow when adding receipt value {} to total unaggregated fees {} \
                            for allocation {} and sender {}. Setting total unaggregated fees to \
                            u128::MAX.",
                            fees, state.value, self.allocation_id, self.sender
                        );
                        u128::MAX
                    });
                    self.sender_account_ref
                        .cast(SenderAccountMessage::UpdateReceiptFees(
                            self.allocation_id,
                            state.clone(),
                        ))?;
                }
            }
            SenderAllocationMessage::TriggerRAVRequest(reply) => {
                self.rav_requester_single().await.map_err(|e| {
                    anyhow! {
                        "Error while requesting RAV for sender {} and allocation {}: {}",
                        self.sender,
                        self.allocation_id,
                        e
                    }
                })?;
                *state = self.calculate_unaggregated_fee().await?;
                if !reply.is_closed() {
                    let _ = reply.send(state.clone());
                }
            }

            SenderAllocationMessage::CloseAllocation => {
                self.rav_requester_single().await.inspect_err(|e| {
                    error!(
                        "Error while requesting RAV for sender {} and allocation {}: {}",
                        self.sender, self.allocation_id, e
                    );
                })?;
                self.mark_rav_final().await.inspect_err(|e| {
                    error!(
                        "Error while marking allocation {} as final for sender {}: {}",
                        self.allocation_id, self.sender, e
                    );
                })?;
                myself.stop(None);
            }

            #[cfg(test)]
            SenderAllocationMessage::GetUnaggregatedReceipts(reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(state.clone());
                }
            }
        }
        Ok(())
    }
}

impl SenderAllocation {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: &'static config::Cli,
        pgpool: PgPool,
        allocation_id: Address,
        sender: Address,
        escrow_accounts: Eventual<EscrowAccounts>,
        escrow_subgraph: &'static SubgraphClient,
        escrow_adapter: EscrowAdapter,
        tap_eip712_domain_separator: Eip712Domain,
        sender_aggregator_endpoint: String,
        sender_account_ref: ActorRef<SenderAccountMessage>,
    ) -> Self {
        let required_checks: Vec<Arc<dyn Check + Send + Sync>> = vec![
            Arc::new(AllocationId::new(
                sender,
                allocation_id,
                escrow_subgraph,
                config,
            )),
            Arc::new(Signature::new(
                tap_eip712_domain_separator.clone(),
                escrow_accounts.clone(),
            )),
        ];
        let context = TapAgentContext::new(
            pgpool.clone(),
            allocation_id,
            sender,
            escrow_accounts.clone(),
            escrow_adapter,
        );
        let tap_manager = TapManager::new(
            tap_eip712_domain_separator.clone(),
            context,
            Checks::new(required_checks),
        );

        Self {
            pgpool,
            tap_manager,
            allocation_id,
            sender,
            sender_aggregator_endpoint,
            config,
            escrow_accounts,
            tap_eip712_domain_separator,
            sender_account_ref,
        }
    }

    /// Delete obsolete receipts in the DB w.r.t. the last RAV in DB, then update the tap manager
    /// with the latest unaggregated fees from the database.
    async fn calculate_unaggregated_fee(&self) -> Result<UnaggregatedReceipts> {
        self.tap_manager.remove_obsolete_receipts().await?;

        let signers = signers_trimmed(&self.escrow_accounts, self.sender).await?;

        // TODO: Get `rav.timestamp_ns` from the TAP Manager's RAV storage adapter instead?
        let res = sqlx::query!(
            r#"
            WITH rav AS (
                SELECT 
                    timestamp_ns 
                FROM 
                    scalar_tap_ravs 
                WHERE 
                    allocation_id = $1 
                    AND sender_address = $2
            ) 
            SELECT 
                MAX(id), 
                SUM(value) 
            FROM 
                scalar_tap_receipts 
            WHERE 
                allocation_id = $1 
                AND signer_address IN (SELECT unnest($3::text[]))
                AND CASE WHEN (
                    SELECT 
                        timestamp_ns :: NUMERIC 
                    FROM 
                        rav
                ) IS NOT NULL THEN timestamp_ns > (
                    SELECT 
                        timestamp_ns :: NUMERIC 
                    FROM 
                        rav
                ) ELSE TRUE END
            "#,
            self.allocation_id.encode_hex::<String>(),
            self.sender.encode_hex::<String>(),
            &signers
        )
        .fetch_one(&self.pgpool)
        .await?;

        ensure!(
            res.sum.is_none() == res.max.is_none(),
            "Exactly one of SUM(value) and MAX(id) is null. This should not happen."
        );

        Ok(UnaggregatedReceipts {
            last_id: res.max.unwrap_or(0).try_into()?,
            value: res
                .sum
                .unwrap_or(BigDecimal::from(0))
                .to_string()
                .parse::<u128>()?,
        })
    }

    /// Request a RAV from the sender's TAP aggregator. Only one RAV request will be running at a
    /// time through the use of an internal guard.
    async fn rav_requester_single(&self) -> Result<()> {
        let RAVRequest {
            valid_receipts,
            previous_rav,
            invalid_receipts,
            expected_rav,
        } = self
            .tap_manager
            .create_rav_request(
                self.config.tap.rav_request_timestamp_buffer_ms * 1_000_000,
                // TODO: limit the number of receipts to aggregate per request.
                None,
            )
            .await
            .map_err(|e| match e {
                tap_core::Error::NoValidReceiptsForRAVRequest => anyhow!(
                    "It looks like there are no valid receipts for the RAV request.\
                 This may happen if your `rav_request_trigger_value` is too low \
                 and no receipts were found outside the `rav_request_timestamp_buffer_ms`.\
                 You can fix this by increasing the `rav_request_trigger_value`."
                ),
                _ => e.into(),
            })?;
        if !invalid_receipts.is_empty() {
            warn!(
                "Found {} invalid receipts for allocation {} and sender {}.",
                invalid_receipts.len(),
                self.allocation_id,
                self.sender
            );

            // Save invalid receipts to the database for logs.
            // TODO: consider doing that in a spawned task?
            Self::store_invalid_receipts(self, invalid_receipts.as_slice()).await?;
        }
        let client = HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(
                self.config.tap.rav_request_timeout_secs,
            ))
            .build(&self.sender_aggregator_endpoint)?;
        let response: JsonRpcResponse<EIP712SignedMessage<ReceiptAggregateVoucher>> = client
            .request(
                "aggregate_receipts",
                rpc_params!(
                    "0.0", // TODO: Set the version in a smarter place.
                    valid_receipts,
                    previous_rav
                ),
            )
            .await?;
        if let Some(warnings) = response.warnings {
            warn!("Warnings from sender's TAP aggregator: {:?}", warnings);
        }
        match self
            .tap_manager
            .verify_and_store_rav(expected_rav.clone(), response.data.clone())
            .await
        {
            Ok(_) => {}

            // Adapter errors are local software errors. Shouldn't be a problem with the sender.
            Err(tap_core::Error::AdapterError { source_error: e }) => {
                anyhow::bail!("TAP Adapter error while storing RAV: {:?}", e)
            }

            // The 3 errors below signal an invalid RAV, which should be about problems with the
            // sender. The sender could be malicious.
            Err(
                e @ tap_core::Error::InvalidReceivedRAV {
                    expected_rav: _,
                    received_rav: _,
                }
                | e @ tap_core::Error::SignatureError(_)
                | e @ tap_core::Error::InvalidRecoveredSigner { address: _ },
            ) => {
                Self::store_failed_rav(self, &expected_rav, &response.data, &e.to_string()).await?;
                anyhow::bail!("Invalid RAV, sender could be malicious: {:?}.", e);
            }

            // All relevant errors should be handled above. If we get here, we forgot to handle
            // an error case.
            Err(e) => {
                anyhow::bail!("Error while verifying and storing RAV: {:?}", e);
            }
        }
        Ok(())
    }

    pub async fn mark_rav_last(&self) -> Result<()> {
        let updated_rows = sqlx::query!(
            r#"
                        UPDATE scalar_tap_ravs
                        SET last = true
                        WHERE allocation_id = $1 AND sender_address = $2
                    "#,
            self.allocation_id.encode_hex::<String>(),
            self.sender.encode_hex::<String>(),
        )
        .execute(&self.pgpool)
        .await?;
        if updated_rows.rows_affected() != 1 {
            anyhow::bail!(
                "Expected exactly one row to be updated in the latest RAVs table, \
                        but {} were updated.",
                updated_rows.rows_affected()
            );
        };
        Ok(())
    }

    async fn store_invalid_receipts(&self, receipts: &[ReceiptWithState<Failed>]) -> Result<()> {
        for received_receipt in receipts.iter() {
            let receipt = received_receipt.signed_receipt();
            let allocation_id = receipt.message.allocation_id;
            let encoded_signature = receipt.signature.to_vec();

            let receipt_signer = receipt
                .recover_signer(&self.tap_eip712_domain_separator)
                .map_err(|e| {
                    error!("Failed to recover receipt signer: {}", e);
                    anyhow!(e)
                })?;

            sqlx::query!(
                r#"
                    INSERT INTO scalar_tap_receipts_invalid (
                        signer_address,
                        signature,
                        allocation_id,
                        timestamp_ns,
                        nonce,
                        value
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                "#,
                receipt_signer.encode_hex::<String>(),
                encoded_signature,
                allocation_id.encode_hex::<String>(),
                BigDecimal::from(receipt.message.timestamp_ns),
                BigDecimal::from(receipt.message.nonce),
                BigDecimal::from(BigInt::from(receipt.message.value)),
            )
            .execute(&self.pgpool)
            .await
            .map_err(|e| anyhow!("Failed to store failed receipt: {:?}", e))?;
        }

        Ok(())
    }

    async fn store_failed_rav(
        &self,
        expected_rav: &ReceiptAggregateVoucher,
        rav: &EIP712SignedMessage<ReceiptAggregateVoucher>,
        reason: &str,
    ) -> Result<()> {
        sqlx::query!(
            r#"
                INSERT INTO scalar_tap_rav_requests_failed (
                    allocation_id,
                    sender_address,
                    expected_rav,
                    rav_response,
                    reason
                )
                VALUES ($1, $2, $3, $4, $5)
            "#,
            self.allocation_id.encode_hex::<String>(),
            self.sender.encode_hex::<String>(),
            serde_json::to_value(expected_rav)?,
            serde_json::to_value(rav)?,
            reason
        )
        .execute(&self.pgpool)
        .await
        .map_err(|e| anyhow!("Failed to store failed RAV: {:?}", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use indexer_common::subgraph_client::DeploymentDetails;
    use ractor::call;
    use serde_json::json;
    use tap_aggregator::server::run_server;

    use wiremock::{
        matchers::{body_string_contains, method},
        Mock, MockServer, ResponseTemplate,
    };

    struct MockSenderAccount;

    #[async_trait::async_trait]
    impl Actor for MockSenderAccount {
        type Msg = SenderAccountMessage;
        type State = ();
        type Arguments = ();

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            _allocation_ids: Self::Arguments,
        ) -> std::result::Result<Self::State, ActorProcessingErr> {
            Ok(())
        }
    }

    use super::*;
    use crate::tap::test_utils::{
        create_rav, create_received_receipt, store_rav, store_receipt, ALLOCATION_ID_0, INDEXER,
        SENDER, SIGNER, TAP_EIP712_DOMAIN_SEPARATOR,
    };

    const DUMMY_URL: &str = "http://localhost:1234";

    async fn create_sender_allocation(
        pgpool: PgPool,
        sender_aggregator_endpoint: String,
        escrow_subgraph_endpoint: &str,
    ) -> ActorRef<SenderAllocationMessage> {
        let config = Box::leak(Box::new(config::Cli {
            config: None,
            ethereum: config::Ethereum {
                indexer_address: INDEXER.1,
            },
            tap: config::Tap {
                rav_request_trigger_value: 100,
                rav_request_timestamp_buffer_ms: 1,
                rav_request_timeout_secs: 5,
                ..Default::default()
            },
            ..Default::default()
        }));

        let escrow_subgraph = Box::leak(Box::new(SubgraphClient::new(
            reqwest::Client::new(),
            None,
            DeploymentDetails::for_query_url(escrow_subgraph_endpoint).unwrap(),
        )));

        let escrow_accounts_eventual = Eventual::from_value(EscrowAccounts::new(
            HashMap::from([(SENDER.1, 1000.into())]),
            HashMap::from([(SENDER.1, vec![SIGNER.1])]),
        ));

        let escrow_adapter = EscrowAdapter::new(escrow_accounts_eventual.clone(), SENDER.1);

        let (sender_account_ref, _join_handle) =
            MockSenderAccount::spawn(None, MockSenderAccount, ())
                .await
                .unwrap();

        let allocation = SenderAllocation::new(
            config,
            pgpool.clone(),
            *ALLOCATION_ID_0,
            SENDER.1,
            escrow_accounts_eventual,
            escrow_subgraph,
            escrow_adapter,
            TAP_EIP712_DOMAIN_SEPARATOR.clone(),
            sender_aggregator_endpoint,
            sender_account_ref,
        )
        .await;

        let (allocation_ref, _join_handle) =
            SenderAllocation::spawn(None, allocation, ()).await.unwrap();

        allocation_ref
    }

    /// Test that the sender_allocation correctly updates the unaggregated fees from the
    /// database when there is no RAV in the database.
    ///
    /// The sender_allocation should consider all receipts found for the allocation and
    /// sender.
    #[sqlx::test(migrations = "../migrations")]
    async fn test_update_unaggregated_fees_no_rav(pgpool: PgPool) {
        // Add receipts to the database.
        for i in 1..10 {
            let receipt =
                create_received_receipt(&ALLOCATION_ID_0, &SIGNER.0, i, i, i.into()).await;
            store_receipt(&pgpool, receipt.signed_receipt())
                .await
                .unwrap();
        }

        let sender_allocation =
            create_sender_allocation(pgpool.clone(), DUMMY_URL.to_string(), DUMMY_URL).await;

        // Get total_unaggregated_fees
        let total_unaggregated_fees = call!(
            sender_allocation,
            SenderAllocationMessage::GetUnaggregatedReceipts
        )
        .unwrap();

        // Check that the unaggregated fees are correct.
        assert_eq!(total_unaggregated_fees.value, 45u128);
    }

    /// Test that the sender_allocation correctly updates the unaggregated fees from the
    /// database when there is a RAV in the database as well as receipts which timestamp are lesser
    /// and greater than the RAV's timestamp.
    ///
    /// The sender_allocation should only consider receipts with a timestamp greater
    /// than the RAV's timestamp.
    #[sqlx::test(migrations = "../migrations")]
    async fn test_update_unaggregated_fees_with_rav(pgpool: PgPool) {
        // Add the RAV to the database.
        // This RAV has timestamp 4. The sender_allocation should only consider receipts
        // with a timestamp greater than 4.
        let signed_rav = create_rav(*ALLOCATION_ID_0, SIGNER.0.clone(), 4, 10).await;
        store_rav(&pgpool, signed_rav, SENDER.1).await.unwrap();

        // Add receipts to the database.
        for i in 1..10 {
            let receipt =
                create_received_receipt(&ALLOCATION_ID_0, &SIGNER.0, i, i, i.into()).await;
            store_receipt(&pgpool, receipt.signed_receipt())
                .await
                .unwrap();
        }

        let sender_allocation =
            create_sender_allocation(pgpool.clone(), DUMMY_URL.to_string(), DUMMY_URL).await;

        // Get total_unaggregated_fees
        let total_unaggregated_fees = call!(
            sender_allocation,
            SenderAllocationMessage::GetUnaggregatedReceipts
        )
        .unwrap();

        // Check that the unaggregated fees are correct.
        assert_eq!(total_unaggregated_fees.value, 35u128);
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_rav_requester_manual(pgpool: PgPool) {
        // Start a TAP aggregator server.
        let (handle, aggregator_endpoint) = run_server(
            0,
            SIGNER.0.clone(),
            vec![SIGNER.1].into_iter().collect(),
            TAP_EIP712_DOMAIN_SEPARATOR.clone(),
            100 * 1024,
            100 * 1024,
            1,
        )
        .await
        .unwrap();

        // Start a mock graphql server using wiremock
        let mock_server = MockServer::start().await;

        // Mock result for TAP redeem txs for (allocation, sender) pair.
        mock_server
            .register(
                Mock::given(method("POST"))
                    .and(body_string_contains("transactions"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(json!({ "data": { "transactions": []}})),
                    ),
            )
            .await;

        // Add receipts to the database.
        for i in 0..10 {
            let receipt =
                create_received_receipt(&ALLOCATION_ID_0, &SIGNER.0, i, i + 1, i.into()).await;
            store_receipt(&pgpool, receipt.signed_receipt())
                .await
                .unwrap();
        }

        // Create a sender_allocation.
        let sender_allocation = create_sender_allocation(
            pgpool.clone(),
            "http://".to_owned() + &aggregator_endpoint.to_string(),
            &mock_server.uri(),
        )
        .await;

        // Trigger a RAV request manually.

        // Get total_unaggregated_fees
        let total_unaggregated_fees = call!(
            sender_allocation,
            SenderAllocationMessage::TriggerRAVRequest
        )
        .unwrap();

        // Check that the unaggregated fees are correct.
        assert_eq!(total_unaggregated_fees.value, 0u128);

        // Stop the TAP aggregator server.
        handle.stop().unwrap();
        handle.stopped().await;
    }
}