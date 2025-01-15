use std::time::Duration;

use mempool::BlockNumber;

pub mod test_utils;

pub mod batch_builder;
pub mod block_builder;
mod domain;
mod errors;
mod mempool;
mod store;

pub mod block;
pub mod config;
pub mod server;

// CONSTANTS
// =================================================================================================

/// The name of the block producer component
pub const COMPONENT: &str = "miden-block-producer";

/// The number of transactions per batch
const SERVER_MAX_TXS_PER_BATCH: usize = 2;

/// The frequency at which blocks are produced
const SERVER_BLOCK_FREQUENCY: Duration = Duration::from_secs(5);

/// The frequency at which batches are built
const SERVER_BUILD_BATCH_FREQUENCY: Duration = Duration::from_secs(2);

/// Maximum number of batches per block
const SERVER_MAX_BATCHES_PER_BLOCK: usize = 4;

/// The number of blocks of committed state that the mempool retains.
///
/// This determines the grace period incoming transactions have between fetching their input from
/// the store and verification in the mempool.
const SERVER_MEMPOOL_STATE_RETENTION: usize = 5;

/// Transactions are rejected by the mempool if there is less than this amount of blocks between the
/// chain tip and the transaction's expiration block.
///
/// This rejects transactions which would likely expire before making it into a block.
const SERVER_MEMPOOL_EXPIRATION_SLACK: BlockNumber = BlockNumber::new(2);

const _: () = assert!(
    SERVER_MAX_BATCHES_PER_BLOCK <= miden_objects::MAX_BATCHES_PER_BLOCK,
    "Server constraint cannot exceed the protocol's constraint"
);

const _: () = assert!(
    SERVER_MAX_TXS_PER_BATCH <= miden_objects::MAX_ACCOUNTS_PER_BATCH,
    "Server constraint cannot exceed the protocol's constraint"
);
