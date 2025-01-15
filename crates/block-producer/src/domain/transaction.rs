use std::{collections::BTreeSet, sync::Arc};

use miden_objects::{
    accounts::AccountId,
    notes::{NoteId, Nullifier},
    transaction::{ProvenTransaction, TransactionId, TxAccountUpdate},
    Digest,
};

use crate::{errors::VerifyTxError, mempool::BlockNumber, store::TransactionInputs};

/// A transaction who's proof has been verified, and which has been authenticated against the store.
///
/// Authentication ensures that all nullifiers are unspent, and additionally authenticates some
/// previously unauthenticated input notes.
///
/// This struct is cheap to clone as it uses an Arc for the heavy data.
///
/// Note that this is of course only valid for the chain height of the authentication.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthenticatedTransaction {
    inner: Arc<ProvenTransaction>,
    /// The account state provided by the store [inputs](TransactionInputs).
    ///
    /// This does not necessarily have to match the transaction's initial state
    /// as this may still be modified by inflight transactions.
    store_account_state: Option<Digest>,
    /// Unauthenticated notes that have now been authenticated by the store
    /// [inputs](TransactionInputs).
    ///
    /// In other words, notes which were unauthenticated at the time the transaction was proven,
    /// but which have since been committed to, and authenticated by the store.
    notes_authenticated_by_store: BTreeSet<NoteId>,
    /// Chain height that the authentication took place at.
    authentication_height: BlockNumber,
}

impl AuthenticatedTransaction {
    /// Verifies the transaction against the inputs, enforcing that all nullifiers are unspent.
    ///
    /// __No__ proof verification is performed. The caller takes responsibility for ensuring
    /// that the proof is valid.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the transaction's nullifiers are marked as spent by the inputs.
    pub fn new(
        tx: ProvenTransaction,
        inputs: TransactionInputs,
    ) -> Result<AuthenticatedTransaction, VerifyTxError> {
        let nullifiers_already_spent = tx
            .get_nullifiers()
            .filter(|nullifier| inputs.nullifiers.get(nullifier).cloned().flatten().is_some())
            .collect::<Vec<_>>();
        if !nullifiers_already_spent.is_empty() {
            return Err(VerifyTxError::InputNotesAlreadyConsumed(nullifiers_already_spent));
        }

        Ok(AuthenticatedTransaction {
            inner: Arc::new(tx),
            notes_authenticated_by_store: inputs.found_unauthenticated_notes,
            authentication_height: BlockNumber::new(inputs.current_block_height),
            store_account_state: inputs.account_hash,
        })
    }

    pub fn id(&self) -> TransactionId {
        self.inner.id()
    }

    pub fn account_id(&self) -> AccountId {
        self.inner.account_id()
    }

    pub fn account_update(&self) -> &TxAccountUpdate {
        self.inner.account_update()
    }

    pub fn store_account_state(&self) -> Option<Digest> {
        self.store_account_state
    }

    pub fn authentication_height(&self) -> BlockNumber {
        self.authentication_height
    }

    pub fn nullifiers(&self) -> impl Iterator<Item = Nullifier> + '_ {
        self.inner.get_nullifiers()
    }

    pub fn output_notes(&self) -> impl Iterator<Item = NoteId> + '_ {
        self.inner.output_notes().iter().map(|note| note.id())
    }

    pub fn output_note_count(&self) -> usize {
        self.inner.output_notes().num_notes()
    }

    pub fn input_note_count(&self) -> usize {
        self.inner.input_notes().num_notes()
    }

    /// Notes which were unauthenticate in the transaction __and__ which were
    /// not authenticated by the store inputs.
    pub fn unauthenticated_notes(&self) -> impl Iterator<Item = NoteId> + '_ {
        self.inner
            .get_unauthenticated_notes()
            .cloned()
            .map(|header| header.id())
            .filter(|note_id| !self.notes_authenticated_by_store.contains(note_id))
    }

    pub fn raw_proven_transaction(&self) -> &ProvenTransaction {
        &self.inner
    }

    pub fn expires_at(&self) -> BlockNumber {
        BlockNumber::new(self.inner.expiration_block_num())
    }
}

impl AuthenticatedTransaction {
    //! Builder methods intended for easier test setup.

    /// Short-hand for `Self::new` where the input's are setup to match the transaction's initial
    /// account state. This covers the account's initial state and nullifiers being set to unspent.
    pub fn from_inner(inner: ProvenTransaction) -> Self {
        let store_account_state = match inner.account_update().init_state_hash() {
            zero if zero == Digest::default() => None,
            non_zero => Some(non_zero),
        };
        let inputs = TransactionInputs {
            account_id: inner.account_id(),
            account_hash: store_account_state,
            nullifiers: inner.get_nullifiers().map(|nullifier| (nullifier, None)).collect(),
            found_unauthenticated_notes: Default::default(),
            current_block_height: Default::default(),
        };
        // SAFETY: nullifiers were set to None aka are definitely unspent.
        Self::new(inner, inputs).unwrap()
    }

    /// Overrides the authentication height with the given value.
    pub fn with_authentication_height(mut self, height: u32) -> Self {
        self.authentication_height = BlockNumber::new(height);
        self
    }

    /// Overrides the store state with the given value.
    pub fn with_store_state(mut self, state: Digest) -> Self {
        self.store_account_state = Some(state);
        self
    }

    /// Unsets the store state.
    pub fn with_empty_store_state(mut self) -> Self {
        self.store_account_state = None;
        self
    }
}
