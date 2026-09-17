use celeriant_crypto::Crypto;
use std::fs;

#[test]
fn generate_writes_only_the_private_key() {
    let dir = tempfile::tempdir().unwrap();
    Crypto::load_or_generate_node_id(dir.path()).unwrap();

    let written: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(written, ["private_key"]);
}

#[test]
fn identity_is_stable_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let original = Crypto::load_or_generate_node_id(dir.path()).unwrap();

    assert_eq!(
        Crypto::load_or_generate_node_id(dir.path()).unwrap(),
        original
    );
}

#[test]
fn truncated_private_key_errors_instead_of_regenerating() {
    let dir = tempfile::tempdir().unwrap();
    Crypto::load_or_generate_node_id(dir.path()).unwrap();

    let private_key_path = dir.path().join("private_key");
    let good = fs::read_to_string(&private_key_path).unwrap();
    fs::write(&private_key_path, &good[..good.len() / 2]).unwrap();

    assert!(Crypto::load_or_generate_node_id(dir.path()).is_err());
}

#[test]
fn unreadable_private_key_errors_instead_of_regenerating() {
    let dir = tempfile::tempdir().unwrap();
    Crypto::load_or_generate_node_id(dir.path()).unwrap();

    fs::write(dir.path().join("private_key"), "not base64 at all!!").unwrap();

    assert!(Crypto::load_or_generate_node_id(dir.path()).is_err());
}

#[test]
fn private_key_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    Crypto::load_or_generate_node_id(dir.path()).unwrap();

    let mode = fs::metadata(dir.path().join("private_key"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "mode was {:#o}", mode & 0o777);
}
