# eventplanedb
A database to store your events

# Linux build
cargo build --release --features mimalloc

# Transfer to cs
rm -rf target
scp -r * cs:/home/utilitydelta/rustsrc