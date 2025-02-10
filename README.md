# Myth Loader

Standalone loadtesting tool for Muse dev/test-nets.

## Usage

* Create and fill the `config.toml` file per the [source](blob/develop/src/config.rs).
  * `fee_signer_pk` must be set to the secret of `marketplace.fee_signer`.
  * Make sure `users_count` is at least twice as big as `senders_count`.
  * A value for `batch_size` that reliably works without hitting weight/proof size limit is `1000`.
* Set the `CONFIG_PATH` environment variable with the path to the config file.
* Run `cargo run --release` or the compiled binary directly.

