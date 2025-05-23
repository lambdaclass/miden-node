use std::collections::BTreeMap;

use miden_objects::{
    block::BlockHeader,
    note::{NoteId, NoteInclusionProof},
    transaction::ChainMmr,
    utils::{Deserializable, Serializable},
};

use crate::{
    errors::{ConversionError, MissingFieldHelper},
    generated::responses as proto,
};

/// Data required for a transaction batch.
#[derive(Clone, Debug)]
pub struct BatchInputs {
    pub batch_reference_block_header: BlockHeader,
    pub note_proofs: BTreeMap<NoteId, NoteInclusionProof>,
    pub chain_mmr: ChainMmr,
}

impl From<BatchInputs> for proto::GetBatchInputsResponse {
    fn from(inputs: BatchInputs) -> Self {
        Self {
            batch_reference_block_header: Some(inputs.batch_reference_block_header.into()),
            note_proofs: inputs.note_proofs.iter().map(Into::into).collect(),
            chain_mmr: inputs.chain_mmr.to_bytes(),
        }
    }
}

impl TryFrom<proto::GetBatchInputsResponse> for BatchInputs {
    type Error = ConversionError;

    fn try_from(response: proto::GetBatchInputsResponse) -> Result<Self, ConversionError> {
        let result = Self {
            batch_reference_block_header: response
                .batch_reference_block_header
                .ok_or(proto::GetBatchInputsResponse::missing_field("block_header"))?
                .try_into()?,
            note_proofs: response
                .note_proofs
                .iter()
                .map(<(NoteId, NoteInclusionProof)>::try_from)
                .collect::<Result<_, ConversionError>>()?,
            chain_mmr: ChainMmr::read_from_bytes(&response.chain_mmr)
                .map_err(|source| ConversionError::deserialization_error("ChainMmr", source))?,
        };

        Ok(result)
    }
}
