# zmtpmini

Minimal async ZMTP client: connect-side DEALER and SUB sockets over TCP

## Development

```bash
cargo test
```

## Release

```bash
cargo test
ship-release
```

`ship-release` tags the Cargo version and pushes; CI publishes to crates.io via trusted publishing and creates the GitHub release, then fastship bumps `Cargo.toml`.

First release only: publish manually with a token (`cargo publish`), then add this repo's `ci.yml` as a trusted publisher in the crate's crates.io settings; later tags publish tokenlessly via CI.
