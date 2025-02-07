use std::{path::Path, fs};

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub node_url: String,

    pub pot_pk: String,
    pub master_pk: String,
    pub collection_admin_pk: String,
    pub fee_signer_pk: String,

    pub batch_size: usize,

    pub senders_count: usize,
    pub sender_funds: u64,

    pub users_count: usize,
    pub user_funds: u64,
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, eyre::Report> {
        let config_string = fs::read_to_string(path)?;
        Ok(toml::de::from_str(&config_string)?)
    }
}

