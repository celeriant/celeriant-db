# EventPlane Client

This project is a simple Rust console application that communicates with an EventPlane server over TCP. It allows users to append events and read responses from the server, demonstrating basic write and read operations.

## Project Structure

```
eventplane_client
├── src
│   ├── main.rs        # Entry point of the application
│   └── protocol.rs    # Protocol definitions for communication
├── Cargo.toml         # Cargo configuration file
└── README.md          # Project documentation
```

## Dependencies

This project uses the following dependencies:

- `tokio`: Asynchronous runtime for Rust.
- `rmp-serde`: MessagePack serialization and deserialization.
- `serde`: Framework for serializing and deserializing Rust data structures.

## Building the Project

To build the project, ensure you have Rust and Cargo installed. Then, navigate to the project directory and run:

```bash
cargo build
```

## Running the Application

To run the application, use the following command:

```bash
cargo run
```

Make sure the EventPlane server is running and accessible at the specified address in the code.

## Functionality

The application establishes a TCP connection to the EventPlane server, sends requests to append events, and reads responses to verify the operations. It handles basic error management and prints the results to the console.

## Contributing

Feel free to fork the repository and submit pull requests for any improvements or bug fixes.