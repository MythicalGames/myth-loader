mod substrate;
mod config;

#[subxt::subxt(
    runtime_metadata_path = "artifacts/muse-dev-metadata.scale",
    derive_for_all_types = "Clone",
    generate_docs,
)]
mod myth {}

use std::{
    env, marker::PhantomData, time::Duration, cell::RefCell, mem,
};

use myth::runtime_types::{
    pallet_marketplace::types::{Execution, Order, OrderType, SignatureData},
    primitive_types::U256,
    runtime_common::IncrementableU256
};
use tracing_subscriber::prelude::*;
use subxt::{
    backend::rpc::reconnecting_rpc_client::{ExponentialBackoff, RpcClient},
    ext::subxt_core::utils::AccountId20,
    OnlineClient,
};
use subxt_signer::eth::Keypair;
use rand::prelude::*;
use futures_lite::{prelude::*, stream};
use futures_buffered::{BufferedStreamExt, BufferedTryStreamExt};
use parity_scale_codec::Encode;

use crate::{
    substrate::MythConfig,
    config::Config,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), eyre::Report> {
    if let Err(e) = dotenvy::dotenv() {
        eprintln!("dotenv: {e}");
    }

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_error::ErrorLayer::default())
        .try_init()?;

    color_eyre::install()?;

    let local = tokio::task::LocalSet::new();
    local.run_until(run()).await
}

async fn run() -> Result<(), eyre::Report> {
    let mut rng = rand::rng();

    let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
    let config = Config::from_file(&config_path)?;

    let rpc = RpcClient::builder()
        .retry_policy(ExponentialBackoff::from_millis(100).max_delay(Duration::from_secs(10)).take(10))
        .build(config.node_url.clone())
        .await?;

    let api: OnlineClient<MythConfig> = OnlineClient::from_rpc_client(rpc.clone()).await.unwrap();

    let ctx = LoadContext::setup(&api, &config, &mut rng).await?;
    ctx.run(&api, &config).await?;

    Ok(())
}

const COIN: u128                = 1__000_000_000_000_000_000;
const CENT: u128                =     10_000_000_000_000_000;
const EXISTENTIAL_DEPOSIT: u128 =     10_000_000_000_000_000;

struct LoadContext {
    pot: Keypair,
    master: Keypair,
    fee_signer: Keypair,
    collection_admin: Keypair,
    collection_id: IncrementableU256,
    senders: Vec<Keypair>,
    users: Vec<Keypair>,
}

impl LoadContext {
    async fn setup(
        api: &OnlineClient<MythConfig>,
        config: &Config,
        rng: &mut impl Rng,
    ) -> Result<Self, eyre::Report> {
        let pot = decode_secret_key(&config.pot_pk)?;
        let master = decode_secret_key(&config.master_pk)?;
        let fee_signer = decode_secret_key(&config.fee_signer_pk)?;
        let collection_admin = decode_secret_key(&config.collection_admin_pk)?;

        let mut senders = vec![];
        let mut users = vec![];

        for _ in 0..config.senders_count {
            let seed = rng.random::<[u8; 64]>();
            let kp = Keypair::from_seed(&seed[..])?;
            senders.push(kp);
        }

        for _ in 0..config.users_count {
            let seed = rng.random::<[u8; 64]>();
            let kp = Keypair::from_seed(&seed[..])?;
            users.push(kp);
        }


        tracing::info!("Funding collection admin...");
        let target_balance = 10 * COIN;
        if get_free_balance(api, collection_admin.public_key().to_account_id()).await? < target_balance {
            api.tx().create_signed(
                &myth::tx().balances().transfer_keep_alive(
                    collection_admin.public_key().to_account_id().to_runtime_type(),
                    target_balance,
                ),
                &pot,
                Default::default(),
            )
                .await?
                .submit_and_watch().await?
                .wait_for_finalized_success().await?;
        }


        tracing::info!("Funding master wallet...");
        let target_balance = 20 * CENT * config.senders_count as u128 + 10 * COIN;
        if get_free_balance(api, master.public_key().to_account_id()).await? < target_balance {
            api.tx().create_signed(
                &myth::tx().balances().transfer_keep_alive(
                    master.public_key().to_account_id().to_runtime_type(),
                    target_balance,
                ),
                &pot,
                Default::default(),
            )
                .await?
                .submit_and_watch().await?
                .wait_for_finalized_success().await?;
        }


        tracing::info!("Providing proxy accesses to Senders...");
        stream::iter([&master, &pot, &collection_admin])
            .flat_map(|delegator|
                stream::iter(senders.chunks(config.batch_size).enumerate())
                    .map(move |(i, kps)| (delegator, i, kps))
            )
            .map(|(delegator, i, keypairs)| {
                let senders = &senders;
                async move {
                    let delegator_id = delegator.public_key().to_account_id();
                    tracing::info!("Creating proxies from {delegator_id}, chunk {} of {}", i, (senders.len() - 1) / config.batch_size + 1);

                    let add_proxy_calls = keypairs.iter()
                        .map(|kp| kp.public_key().to_account_id().to_runtime_type())
                        .map(|delegate| myth::myth_proxy::Call::add_proxy {
                            delegate,
                            proxy_type: myth::runtime_types::testnet_runtime::ProxyType::Any,
                            sponsor: None,
                        })
                        .map(|call| myth::Call::MythProxy(call))
                        .collect::<Vec<_>>();

                    api.tx().create_signed(
                        &myth::tx().utility().batch_all(add_proxy_calls),
                        delegator,
                        Default::default(),
                    )
                        .await?
                        .submit_and_watch().await?
                        .wait_for_finalized_success().await?;

                    <Result<(), eyre::Report>>::Ok(())
                }
            })
            .map(Ok)
            .try_buffered_ordered(1000)
            .try_for_each(|_| <Result<(), eyre::Report>>::Ok(()))
            .await?;

        tracing::info!("Creating collection...");
        let create_collection_event = api.tx().create_signed(
            &myth::tx().nfts().create(
                collection_admin.public_key().to_account_id().to_runtime_type(),
                myth::runtime_types::pallet_nfts::types::CollectionConfig {
                    settings: myth::runtime_types::pallet_nfts::types::BitFlags1(0, PhantomData),
                    max_supply: Some(u128::MAX),
                    mint_settings: myth::runtime_types::pallet_nfts::types::MintSettings {
                        mint_type: myth::runtime_types::pallet_nfts::types::MintType::Issuer,
                        price: None,
                        start_block: None,
                        end_block: None,
                        default_item_settings: myth::runtime_types::pallet_nfts::types::BitFlags1(0, PhantomData),
                        serial_mint: true,
                    }
                },
            ),
            &collection_admin,
            Default::default(),
        )
            .await?
            .submit_and_watch().await?
            .wait_for_finalized_success().await?
            .find_first::<myth::nfts::events::Created>()?
            .expect("create() should always emit Created event");

        let collection_id = create_collection_event.collection;


        for (i, keypairs) in senders.chunks(config.batch_size).enumerate() {
            tracing::info!("Funding senders, chunk {i} of {}...", (senders.len() - 1) / config.batch_size + 1);
            let fund_calls = keypairs.iter()
                .map(|kp| kp.public_key().to_account_id().to_runtime_type())
                .map(|dest| myth::balances::Call::transfer_keep_alive {
                    dest,
                    value: config.sender_funds as u128 * COIN,
                })
                .map(|call| myth::Call::Balances(call))
                .collect::<Vec<_>>();
            api.tx().create_signed(
                &myth::tx().utility().batch_all(fund_calls),
                &pot,
                Default::default(),
            )
                .await?
                .submit_and_watch().await?
                .wait_for_finalized_success().await?;
        }


        for (i, keypairs) in users.chunks(config.batch_size).enumerate() {
            tracing::info!("Funding users, chunk {i} of {}...", (users.len() - 1) / config.batch_size + 1);
            let fund_calls = keypairs.iter()
                .map(|kp| kp.public_key().to_account_id().to_runtime_type())
                .map(|dest| myth::balances::Call::transfer_keep_alive {
                    dest,
                    value: config.user_funds as u128 * COIN + 20*CENT + EXISTENTIAL_DEPOSIT,
                })
                .map(|call| myth::Call::Balances(call))
                .collect::<Vec<_>>();
            api.tx().create_signed(
                &myth::tx().utility().batch_all(fund_calls),
                &pot,
                Default::default(),
            )
                .await?
                .submit_and_watch().await?
                .wait_for_finalized_success().await?;
        }


        tracing::info!("Providing users proxy access to Master...");
        stream::iter(users.iter())
            .map(|kp| async {
                let user = kp.public_key().to_account_id();
                tracing::debug!("Creating proxy for user {}...", &user);

                api.tx().create_signed(
                    &myth::tx().myth_proxy().add_proxy(
                        master.public_key().to_account_id().to_runtime_type(),
                        myth::runtime_types::testnet_runtime::ProxyType::Any,
                        None,
                    ),
                    kp,
                    Default::default(),
                ).await?
                .submit_and_watch().await?
                .wait_for_finalized_success().await?;

                tracing::debug!("Creating proxy for user {} done.", &user);
                <Result<_, subxt::Error>>::Ok(())
            })
            .map(|fut| Ok(fut))
            .try_buffered_unordered(1000)
            .try_for_each(|_| <Result<_, subxt::Error>>::Ok(()))
            .await?;


        tracing::info!("Done setting up.");

        Ok(LoadContext {
            pot,
            master,
            fee_signer,
            collection_admin,
            collection_id,
            senders,
            users,
        })
    }

    async fn run(self, api: &OnlineClient<MythConfig>, config: &Config) -> Result<(), eyre::Report> {
        let LoadContext {
            pot,
            master,
            fee_signer,
            collection_admin,
            collection_id,
            senders,
            users,
        } = self;

        let users = RefCell::new(users);

        let senders_count = senders.len();
        stream::iter(senders)
            .map(|sender| async {
                let sender = sender;
                let master_id = master.public_key().to_account_id();
                let sender_id = sender.public_key().to_account_id();

                loop {
                    if get_free_balance(api, sender_id).await.expect("get_free_balance should work") < 5 * COIN {
                        tracing::warn!("Sender {sender_id} ran out of funds, stopping.");
                        break;
                    }

                    let mut users_mut = users.borrow_mut();
                    let alice = users_mut.pop().expect("Users should never run out");
                    let bob = users_mut.pop().expect("Users should never run out");
                    mem::drop(users_mut);

                    let alice_id = alice.public_key().to_account_id();
                    let bob_id = bob.public_key().to_account_id();

                    let item_res = mint(
                        api,
                        &sender,
                        collection_id.clone(),
                        collection_admin.public_key().to_account_id(),
                        alice_id,
                    ).await;
                    let item = match item_res {
                        Ok(item) => item,
                        Err(e) => {
                            tracing::warn!("Failure in mint: {e}");
                            {
                                let mut users = users.borrow_mut();
                                users.push(alice);
                                users.push(bob);
                            }
                            tokio::time::sleep(Duration::from_secs(50)).await;
                            continue;
                        }
                    };

                    let transfer_res = transfer(
                        api,
                        &sender,
                        master_id,
                        collection_id.clone(),
                        item,
                        alice_id,
                        bob_id,
                    ).await;

                    if let Err(e) = transfer_res {
                        tracing::warn!("Failure in transfer: {e}");
                        {
                            let mut users = users.borrow_mut();
                            users.push(alice);
                            users.push(bob);
                        }
                        tokio::time::sleep(Duration::from_secs(50)).await;
                        continue;
                    }

                    let trade_res = trade(
                        api,
                        &pot,
                        &sender,
                        master_id,
                        collection_id.clone(),
                        &bob,
                        &alice,
                        &fee_signer,
                        item,
                        config.user_funds as u128 * COIN,
                    ).await;

                    if let Err(e) = trade_res {
                        tracing::warn!("Failure in trade: {e}");
                        {
                            let mut users = users.borrow_mut();
                            users.push(alice);
                            users.push(bob);
                        }
                        tokio::time::sleep(Duration::from_secs(50)).await;
                        continue;
                    }

                    let burn_res = burn(
                        api,
                        &sender,
                        master_id,
                        collection_id.clone(),
                        alice_id,
                        item,
                    ).await;

                    {
                        let mut users = users.borrow_mut();
                        users.push(alice);
                        users.push(bob);
                    }

                    if let Err(e) = burn_res {
                        tracing::warn!("Failure in burn: {e}");
                        tokio::time::sleep(Duration::from_secs(50)).await;
                        continue;
                    }
                }
            })
            .buffered_unordered(senders_count)
            .for_each(|_|{}).await;

        Ok(())
    }
}


async fn mint(
    api: &OnlineClient<MythConfig>,
    sender: &Keypair,
    collection: IncrementableU256,
    admin: AccountId20,
    recipient: AccountId20,
) -> Result<u128, eyre::Report> {
    let issued = api.tx().create_signed(
        &myth::tx().myth_proxy().proxy(
            admin.to_runtime_type(),
            myth::Call::Nfts(myth::nfts::Call::mint{
                collection: collection,
                maybe_item: None,
                mint_to: recipient.to_runtime_type(),
                witness_data: None,
            }),
        ),
        sender,
        Default::default(),
    ).await?
        .submit_and_watch().await?
        .wait_for_finalized_success().await?
        .find_first::<myth::nfts::events::Issued>()?
        .expect("mint() should always emit Issued event");

    Ok(issued.item)
}

async fn transfer(
    api: &OnlineClient<MythConfig>,
    sender: &Keypair,
    master: AccountId20,
    collection: IncrementableU256,
    item: u128,
    from: AccountId20,
    to: AccountId20,
) -> Result<(), eyre::Report> {
    let transfer_call = myth::Call::Nfts(myth::nfts::Call::transfer{
        collection,
        item,
        dest: to.to_runtime_type(),
    });

    let proxy_inner_call = myth::Call::MythProxy(myth::myth_proxy::Call::proxy{
        address: from.to_runtime_type(),
        call: Box::new(transfer_call),
    });

    api.tx().create_signed(
        &myth::tx().myth_proxy().proxy(
            master.to_runtime_type(),
            proxy_inner_call,
        ),
        sender,
        Default::default(),
    ).await?
        .submit_and_watch().await?
        .wait_for_finalized_success().await?;

    Ok(())
}

async fn trade(
    api: &OnlineClient<MythConfig>,
    _pot: &Keypair,
    sender: &Keypair,
    master: AccountId20,
    collection: IncrementableU256,
    seller: &Keypair,
    buyer: &Keypair,
    fee_signer: &Keypair,
    item: u128,
    price: u128
) -> Result<(), eyre::Report> {
    let seller_id = seller.public_key().to_account_id();
    let buyer_id = buyer.public_key().to_account_id();

    api.tx().create_signed(
        &myth::tx().balances().transfer_keep_alive(buyer_id.to_runtime_type(), price + 1*COIN),
        sender,
        Default::default(),
    ).await?
        .submit_and_watch().await?
        .wait_for_finalized_success().await?;

    api.tx().create_signed(
        &myth::tx().myth_proxy().proxy(
            master.to_runtime_type(),
            myth::Call::MythProxy(myth::myth_proxy::Call::proxy{
                address: seller_id.to_runtime_type(),
                call: Box::new(make_create_order(
                    fee_signer,
                    OrderType::Ask,
                    collection.clone(),
                    item,
                    price,
                    myth_timestamp_now() + 9_000_001,
                    1 * COIN,
                    Execution::AllowCreation,
                )),
            }),
        ),
        sender,
        Default::default(),
    )
        .await?
        .submit_and_watch().await?
        .wait_for_finalized_success().await?;

    api.tx().create_signed(
        &myth::tx().myth_proxy().proxy(
            master.to_runtime_type(),
            myth::Call::MythProxy(myth::myth_proxy::Call::proxy{
                address: buyer_id.to_runtime_type(),
                call: Box::new(make_create_order(
                    fee_signer,
                    OrderType::Bid,
                    collection.clone(),
                    item,
                    price,
                    myth_timestamp_now() + 9_000_001,
                    1 * COIN,
                    Execution::Force,
                )),
            }),
        ),
        sender,
        Default::default(),
    )
        .await?
        .submit_and_watch().await?
        .wait_for_finalized_success().await?;

    api.tx().create_signed(
        &myth::tx().balances().transfer_all(
            sender.public_key().to_account_id().to_runtime_type(),
            true,
        ),
        seller,
        Default::default(),
    )
        .await?
        .submit_and_watch().await?
        .wait_for_finalized_success().await?;

    Ok(())
}

async fn burn(
    api: &OnlineClient<MythConfig>,
    sender: &Keypair,
    master: AccountId20,
    collection: IncrementableU256,
    owner: AccountId20,
    item: u128
) -> Result<(), eyre::Report> {
    let burn_call = myth::Call::Nfts(myth::nfts::Call::burn{
        collection,
        item,
    });

    let proxy_inner_call = myth::Call::MythProxy(myth::myth_proxy::Call::proxy{
        address: owner.to_runtime_type(),
        call: Box::new(burn_call),
    });

    api.tx().create_signed(
        &myth::tx().myth_proxy().proxy(
            master.to_runtime_type(),
            proxy_inner_call,
        ),
        sender,
        Default::default(),
    ).await?
        .submit_and_watch().await?
        .wait_for_finalized_success().await?;

    Ok(())
}

// Helpers

fn decode_secret_key(key_str: &str) -> Result<Keypair, eyre::Report> {
    if key_str.is_empty() {
        tracing::error!("Secret key is empty. Here are some keys to use:");
        let mut rng = rand::rng();

        for _ in 0..3 {
            let seed = rng.random::<[u8; 64]>();
            let kp = Keypair::from_seed(&seed[..])?;

            let secret_hex = hex::encode(&kp.secret_key()[..]);
            let account_id_string = kp.public_key().to_account_id().to_string();

            tracing::error!("{account_id_string} {secret_hex}");
        }

        eyre::bail!("Empty secret key");
    }

    let decoded = hex::decode(key_str)?;
    let Ok(array) = decoded.try_into() else {
        eyre::bail!("Unable to decode {key_str} into 65-byte secret key array");
    };
    let keypair = Keypair::from_secret_key(array)?;
    Ok(keypair)
}

async fn get_free_balance(api: &OnlineClient<MythConfig>, account: AccountId20) -> Result<u128, eyre::Report> {
    let storage_key = myth::storage()
        .system()
        .account(account.to_runtime_type());
    let info = api.storage()
        .at_latest().await?
        .fetch(&storage_key).await?;

    Ok(info.map(|a| a.data.free).unwrap_or(0))
}

fn make_create_order(
    mp_signer: &Keypair,
    order_type: OrderType,
    collection: IncrementableU256,
    item: u128,
    price: u128,
    expires_at: u64,
    fee: u128,
    execution: Execution,
) -> myth::Call {
    use rand::distr::{Alphanumeric, SampleString};

    #[derive(Encode)]
    pub struct OrderMessage {
        pub collection: U256,
        pub item: u128,
        pub price: u128,
        pub expires_at: u64,
        pub fee: u128,
        pub escrow_agent: Option<myth::runtime_types::account::AccountId20>,
        pub nonce: String,
    }

    let nonce: String = Alphanumeric.sample_string(&mut rand::rng(), 9);

    let order_msg = OrderMessage {
        collection: collection.0.clone(),
        item: item.clone(),
        price,
        expires_at,
        fee,
        escrow_agent: None,
        nonce: nonce.clone(),
    };
    let order_bytes = order_msg.encode();
    let signature = mp_signer.sign(&order_bytes[..]);

    myth::Call::Marketplace(myth::marketplace::Call::create_order {
        order: Order {
            order_type: order_type.clone(),
            collection: collection.clone(),
            item: item.clone(),
            price,
            expires_at,
            fee,
            escrow_agent: None,
            signature_data: SignatureData {
                signature: myth::runtime_types::account::EthereumSignature(signature.0),
                nonce: Vec::from(nonce),
            },
        },
        execution,
    })
}

fn myth_timestamp_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time");
    duration.as_millis() as u64
}

trait AccountId20Ext {
    fn to_runtime_type(&self) -> myth::runtime_types::account::AccountId20;
}

impl AccountId20Ext for AccountId20 {
    fn to_runtime_type(&self) -> myth::runtime_types::account::AccountId20 {
        myth::runtime_types::account::AccountId20(self.0)
    }
}

