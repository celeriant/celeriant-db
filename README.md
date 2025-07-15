# eventplanedb
A database to store your events

# Linux build
cargo build --release --features tikv-jemallocator
# Auth / Crypto Notes

Encrypt symmetric key with the user's public key.
That way it can always be retreived