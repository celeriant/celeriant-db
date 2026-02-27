use std::path::PathBuf;
use clap::{Parser, Subcommand};
use base64::{Engine as _, engine::general_purpose};
use celeriant_crypto::{generate_api_key, hash_api_key};

use crate::api_keys::{ApiKeysConfig, load_api_keys, save_api_keys};

#[derive(Debug, Parser)]
#[command(name = "celeriant keys", about = "Manage API keys")]
struct KeysArgs {
    #[command(subcommand)]
    command: KeysCommand,
}

#[derive(Debug, Subcommand)]
enum KeysCommand {
    /// Generate all 4 API keys
    Generate {
        /// Data directory path
        #[arg(long)]
        data_root: PathBuf,

        /// Overwrite existing file
        #[arg(long)]
        force: bool,
    },
    /// Regenerate a single key
    Regenerate {
        /// Key name: primary-rw, secondary-rw, primary-ro, secondary-ro
        key_name: String,

        /// Data directory path
        #[arg(long)]
        data_root: PathBuf,
    },
    /// List configured keys (shows truncated hashes)
    List {
        /// Data directory path
        #[arg(long)]
        data_root: PathBuf,
    },
}

/// Run a `keys` subcommand parsed from argv (everything after "keys").
pub fn run_keys(argv: Vec<String>) -> Result<(), String> {
    let mut args = vec!["celeriant-keys".to_string()];
    args.extend(argv);

    let keys_args = KeysArgs::parse_from(args);
    match keys_args.command {
        KeysCommand::Generate { data_root, force } => {
            generate_keys(&data_root, force)
        }
        KeysCommand::Regenerate { key_name, data_root } => {
            regenerate_key(&data_root, &key_name)
        }
        KeysCommand::List { data_root } => {
            list_keys(&data_root)
        }
    }
}

fn generate_keys(data_root: &PathBuf, force: bool) -> Result<(), String> {
    let api_keys_path = data_root.join("api_keys.toml");

    // Check if file exists
    if api_keys_path.exists() && !force {
        return Err(format!(
            "API keys file already exists: {}\nUse --force to overwrite (all existing keys will become invalid)",
            api_keys_path.display()
        ));
    }

    // Ensure data_root exists
    std::fs::create_dir_all(data_root)
        .map_err(|e| format!("Failed to create data root directory: {}", e))?;

    // Generate 4 keys
    let primary_rw_key = generate_api_key();
    let secondary_rw_key = generate_api_key();
    let primary_ro_key = generate_api_key();
    let secondary_ro_key = generate_api_key();

    // Hash each key
    let keys_config = ApiKeysConfig {
        primary_rw: hash_api_key(&primary_rw_key),
        secondary_rw: hash_api_key(&secondary_rw_key),
        primary_ro: hash_api_key(&primary_ro_key),
        secondary_ro: hash_api_key(&secondary_ro_key),
    };

    // Save to file
    save_api_keys(data_root, &keys_config)
        .map_err(|e| format!("Failed to save API keys: {}", e))?;

    // Print plaintext keys (show once)
    println!("API keys generated successfully.\n");
    println!("  IMPORTANT: Copy these keys now. They cannot be retrieved again.\n");
    println!("  primary_rw:    {}", general_purpose::STANDARD.encode(primary_rw_key));
    println!("  secondary_rw:  {}", general_purpose::STANDARD.encode(secondary_rw_key));
    println!("  primary_ro:    {}", general_purpose::STANDARD.encode(primary_ro_key));
    println!("  secondary_ro:  {}", general_purpose::STANDARD.encode(secondary_ro_key));
    println!("\n  Key hashes written to: {}", api_keys_path.display());
    println!("  Restart the server to enable authentication.");

    Ok(())
}

fn regenerate_key(data_root: &PathBuf, key_name: &str) -> Result<(), String> {
    let api_keys_path = data_root.join("api_keys.toml");

    if !api_keys_path.exists() {
        return Err(format!(
            "API keys file not found: {}\nRun 'celeriant keys generate' first",
            api_keys_path.display()
        ));
    }

    // Load existing keys
    let mut keys = load_api_keys(data_root)
        .map_err(|e| format!("Failed to load API keys: {}", e))?
        .ok_or_else(|| "No API keys found".to_string())?;

    // Generate new key
    let new_key = generate_api_key();
    let new_hash = hash_api_key(&new_key);

    // Update the specified key
    match key_name {
        "primary-rw" => keys.primary_rw = new_hash,
        "secondary-rw" => keys.secondary_rw = new_hash,
        "primary-ro" => keys.primary_ro = new_hash,
        "secondary-ro" => keys.secondary_ro = new_hash,
        _ => {
            return Err(format!(
                "Invalid key name: {}\nValid names: primary-rw, secondary-rw, primary-ro, secondary-ro",
                key_name
            ));
        }
    }

    // Save updated keys
    save_api_keys(data_root, &keys)
        .map_err(|e| format!("Failed to save API keys: {}", e))?;

    // Print new plaintext key (show once)
    println!("Key regenerated successfully.\n");
    println!("  IMPORTANT: Copy this key now. It cannot be retrieved again.\n");
    println!("  {}:  {}", key_name, general_purpose::STANDARD.encode(new_key));
    println!("\n  Key hash updated in: {}", api_keys_path.display());
    println!("  Send SIGHUP to the running server to hot-reload keys.");

    Ok(())
}

fn list_keys(data_root: &PathBuf) -> Result<(), String> {
    let api_keys_path = data_root.join("api_keys.toml");

    if !api_keys_path.exists() {
        return Err(format!(
            "API keys file not found: {}\nRun 'celeriant keys generate' first",
            api_keys_path.display()
        ));
    }

    let keys = load_api_keys(data_root)
        .map_err(|e| format!("Failed to load API keys: {}", e))?
        .ok_or_else(|| "No API keys found".to_string())?;

    println!("\n  Key              Access Level    Hash (first 8 chars)");
    println!("  ─────────────    ─────────────   ────────────────────");

    print_key_info("primary_rw", "ReadWrite", &keys.primary_rw);
    print_key_info("secondary_rw", "ReadWrite", &keys.secondary_rw);
    print_key_info("primary_ro", "ReadOnly", &keys.primary_ro);
    print_key_info("secondary_ro", "ReadOnly", &keys.secondary_ro);
    println!();

    Ok(())
}

fn print_key_info(name: &str, access_level: &str, hash: &[u8; 32]) {
    let hex_hash: String = hash.iter()
        .take(4)
        .map(|b| format!("{:02x}", b))
        .collect();
    println!("  {:<16} {:<15} {}...", name, access_level, hex_hash);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_keys() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_root = temp_dir.path().to_path_buf();

        let result = generate_keys(&data_root, false);
        assert!(result.is_ok());

        let api_keys_path = data_root.join("api_keys.toml");
        assert!(api_keys_path.exists());

        // Verify we can load them
        let loaded = load_api_keys(&data_root).unwrap();
        assert!(loaded.is_some());
    }

    #[test]
    fn test_generate_keys_no_force_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_root = temp_dir.path().to_path_buf();

        // First generation should succeed
        generate_keys(&data_root, false).unwrap();

        // Second should fail without --force
        let result = generate_keys(&data_root, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_generate_keys_with_force() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_root = temp_dir.path().to_path_buf();

        // First generation
        generate_keys(&data_root, false).unwrap();

        // Second with --force should succeed
        let result = generate_keys(&data_root, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_regenerate_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_root = temp_dir.path().to_path_buf();

        // Generate initial keys
        generate_keys(&data_root, false).unwrap();
        let original = load_api_keys(&data_root).unwrap().unwrap();

        // Regenerate primary_rw
        regenerate_key(&data_root, "primary-rw").unwrap();
        let updated = load_api_keys(&data_root).unwrap().unwrap();

        // primary_rw should be different
        assert_ne!(original.primary_rw, updated.primary_rw);
        // Others should be the same
        assert_eq!(original.secondary_rw, updated.secondary_rw);
        assert_eq!(original.primary_ro, updated.primary_ro);
        assert_eq!(original.secondary_ro, updated.secondary_ro);
    }

    #[test]
    fn test_regenerate_invalid_key_name() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_root = temp_dir.path().to_path_buf();

        generate_keys(&data_root, false).unwrap();

        let result = regenerate_key(&data_root, "invalid-key");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid key name"));
    }

    #[test]
    fn test_list_keys() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_root = temp_dir.path().to_path_buf();

        generate_keys(&data_root, false).unwrap();

        let result = list_keys(&data_root);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_keys_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_root = temp_dir.path().to_path_buf();

        let result = list_keys(&data_root);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }
}
