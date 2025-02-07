use subxt::{
    OnlineClient,
    config::substrate::{
        BlakeTwo256, SubstrateHeader, SubstrateExtrinsicParams, H256, U256,
    },
};
use subxt::ext::subxt_core::utils::AccountId20;
use subxt_signer::eth::Signature;

use crate::{
    myth::{self, runtime_types::{primitive_types, runtime_common::IncrementableU256}}
};

pub struct MythConfig;

impl subxt::Config for MythConfig {
    type Hash = H256;
    type AccountId = AccountId20;
    type Address = AccountId20;
    type Signature = Signature;
    type Hasher = BlakeTwo256;
    type Header = SubstrateHeader<u32, BlakeTwo256>;
    type ExtrinsicParams = SubstrateExtrinsicParams<Self>;
    type AssetId = u32;
}

