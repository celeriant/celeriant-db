use celeriant_wal::schema_key::SchemaKey;
use deepsize::DeepSizeOf;
use std::rc::Rc;

pub trait Validate {
    fn validate(&self, event_value: &[u8]) -> Result<(), String>;
}

pub struct CachedValidator<V: Validate> {
    validator: Rc<V>,
    size_estimate: usize,
}

impl<V: Validate> CachedValidator<V> {
    pub fn new(validator: Rc<V>, size_estimate: usize) -> Self {
        Self { validator, size_estimate }
    }

    pub fn validate(&self, event_value: &[u8]) -> Result<(), String> {
        self.validator.validate(event_value)
    }
}

impl<V: Validate> Clone for CachedValidator<V> {
    fn clone(&self) -> Self {
        Self {
            validator: self.validator.clone(),
            size_estimate: self.size_estimate,
        }
    }
}

impl<V: Validate> std::fmt::Debug for CachedValidator<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedValidator")
            .field("size_estimate", &self.size_estimate)
            .finish()
    }
}

impl<V: Validate> DeepSizeOf for CachedValidator<V> {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        self.size_estimate
    }
}

pub enum CachedSchema<V: Validate> {
    Validated(CachedValidator<V>),
    CompilationFailed(String),
}

impl<V: Validate> Clone for CachedSchema<V> {
    fn clone(&self) -> Self {
        match self {
            Self::Validated(v) => Self::Validated(v.clone()),
            Self::CompilationFailed(e) => Self::CompilationFailed(e.clone()),
        }
    }
}

impl<V: Validate> std::fmt::Debug for CachedSchema<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validated(v) => f.debug_tuple("Validated").field(v).finish(),
            Self::CompilationFailed(e) => f.debug_tuple("CompilationFailed").field(e).finish(),
        }
    }
}

impl<V: Validate> DeepSizeOf for CachedSchema<V> {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        match self {
            Self::Validated(v) => v.deep_size_of_children(context),
            Self::CompilationFailed(e) => e.deep_size_of_children(context),
        }
    }
}

/// Collects unique SchemaKeys without allocating for the common case (<=2 unique keys).
pub struct UniqueSchemaKeys {
    inline: [Option<SchemaKey>; 2],
    overflow: Option<Vec<SchemaKey>>,
}

impl UniqueSchemaKeys {
    pub fn new() -> Self {
        Self {
            inline: [None, None],
            overflow: None,
        }
    }

    /// Insert a key. Returns true if key was new (not already present).
    pub fn try_insert(&mut self, key: SchemaKey) -> bool {
        // Check inline slots
        for slot in &self.inline {
            if let Some(existing) = slot {
                if *existing == key {
                    return false;
                }
            }
        }

        // Check overflow
        if let Some(overflow) = &self.overflow {
            if overflow.iter().any(|k| *k == key) {
                return false;
            }
        }

        // Not found — insert into first empty inline slot or overflow
        for slot in &mut self.inline {
            if slot.is_none() {
                *slot = Some(key);
                return true;
            }
        }

        self.overflow.get_or_insert_with(Vec::new).push(key);
        true
    }

    pub fn iter(&self) -> impl Iterator<Item = &SchemaKey> {
        self.inline
            .iter()
            .filter_map(|s| s.as_ref())
            .chain(self.overflow.iter().flat_map(|v| v.iter()))
    }
}
