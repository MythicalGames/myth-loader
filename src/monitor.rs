#![allow(unused_imports)]

use std::{cell::RefCell, collections::HashMap, sync::Arc};

use flume::{Receiver, Sender};
use futures_lite::{prelude::*, stream, StreamExt};
use parity_scale_codec::{Decode, Encode};
use rayon::prelude::*;
use subxt::{
    blocks::{Block, ExtrinsicDetails},
    config::substrate::BlakeTwo256,
    events::{EventDetails, Phase},
    utils::H256,
    OnlineClient,
};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{
    myth::{self, system::events::{ExtrinsicFailed, ExtrinsicSuccess}},
    substrate::MythConfig,
};

pub type ChannelPayload = (bool, Vec<EventDetails<MythConfig>>);

#[derive(Default)]
pub struct ExtrinsicTracker {
    tracked_extrinsics: HashMap<H256, Sender<ChannelPayload>>,
}

impl ExtrinsicTracker {
    pub fn track(&mut self, extrinsic_hash: H256) -> Receiver<ChannelPayload> {
        let (tx, rx) = flume::bounded(1);
        self.tracked_extrinsics.insert(extrinsic_hash, tx);
        rx
    }
}

pub struct Monitor {
    pub tracker: Arc<Mutex<ExtrinsicTracker>>,
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(Mutex::new(ExtrinsicTracker::default())),
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn monitor(self, api: Arc<OnlineClient<MythConfig>>) -> Result<(), eyre::Report> {
        let mut finalized_blocks = api.blocks().subscribe_finalized().await?;
        while let Some(block_res) = finalized_blocks.next().await {
            let block = block_res?;
            tracing::debug!("Processing block {}", block.number());

            let mut extrinsics = block.extrinsics().await?
                .iter()
                .map(|ext| (ext, vec![]))
                .collect::<Vec<_>>();

            let events = block
                .events()
                .await?
                .iter()
                .collect::<Result<Vec<_>, _>>()?;
            /*
            let ext_status_events = events
                .chunk_by(|a, b| a.phase() == b.phase())
                .filter(|evt| matches!(evt.first().unwrap().phase(), Phase::ApplyExtrinsic(_)))
                .map(|chunk| chunk.to_owned());
            */

            for event in events {
                if let Phase::ApplyExtrinsic(index) = event.phase() {
                    extrinsics[index as usize].1.push(event);
                    assert_eq!(extrinsics[index as usize].0.index(), index);
                }
            }

            let locked_tracker = self.tracker.clone().lock_owned().await;

            tokio::task::spawn_blocking(move || {
                process_block(extrinsics, locked_tracker)
            }).await.unwrap()?;
        }

        Ok(())
    }
}

pub fn process_block(
    extrinsics_statuses: Vec<(
        ExtrinsicDetails<MythConfig, OnlineClient<MythConfig>>,
        Vec<EventDetails<MythConfig>>,
    )>,
    mut tracker: OwnedMutexGuard<ExtrinsicTracker>,
) -> Result<(), eyre::Report> {
    use myth::runtime_types::frame_system::pallet::Event;
    let notified = extrinsics_statuses.into_par_iter().map(|(ext, evts)| {
        let ext_hash = ext.hash();
        let evt = evts.last().unwrap();
        match tracker.tracked_extrinsics.get(&ext_hash) {
            Some(chan) => {
                let decoded_evt = evt.as_root_event::<myth::Event>()?;
                match decoded_evt {
                    myth::Event::System(Event::ExtrinsicSuccess{..}) => {
                        if let Err(e) = chan.try_send((true, evts)) {
                            tracing::error!("Unable to notify about extrinsic status: {e}");
                        }
                    }
                    myth::Event::System(Event::ExtrinsicFailed{..}) => {
                        tracing::error!("Tracked extrinsic #{} failed", ext.index());
                        if let Err(e) = chan.try_send((false, evts)) {
                            tracing::error!("Unable to notify about extrinsic status: {e}");
                        }
                    }
                    _ => unreachable!(),
                }
                Ok(Some(ext_hash))
            }
            None => Ok(None),
        }
    })
        .filter_map(|res| res.transpose())
        .collect::<Result<Vec<_>, eyre::Report>>()?;

    for hash in notified {
        tracker.tracked_extrinsics.remove(&hash);
    }

    Ok(())
}
