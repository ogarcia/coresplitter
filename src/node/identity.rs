use anyhow::Result;
use sha2::{Digest, Sha256};

use super::state::NodeState;

const KEY_LEN: usize = 32;

pub struct Identity {
    pub seed: [u8; KEY_LEN],
    pub public_key: [u8; KEY_LEN],
    pub name: String,
    initialized: bool,
}

impl Identity {
    pub fn new(name: String) -> Self {
        Self {
            seed: [0u8; KEY_LEN],
            public_key: [0u8; KEY_LEN],
            name,
            initialized: false,
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn generate(&mut self) {
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        rng.fill_bytes(&mut self.seed);
        self.public_key = derive_public_key(&self.seed);
        self.initialized = true;
        tracing::info!(
            pubkey = %hex::encode(&self.public_key[..8]),
            "generated new identity keypair"
        );
    }

    pub async fn load_from_db(&mut self, state: &NodeState) -> Result<bool> {
        let seed = state.kv_get("identity.seed").await?;
        match seed {
            Some(s) if s.len() >= KEY_LEN => {
                self.seed.copy_from_slice(&s[..KEY_LEN]);
                let pk = state.kv_get("identity.pk").await?;
                if let Some(p) = pk {
                    if p.len() >= KEY_LEN {
                        self.public_key.copy_from_slice(&p[..KEY_LEN]);
                    } else {
                        self.public_key = derive_public_key(&self.seed);
                    }
                } else {
                    self.public_key = derive_public_key(&self.seed);
                }
                self.initialized = true;
                tracing::info!(
                    pubkey = %hex::encode(&self.public_key[..8]),
                    "loaded identity from database"
                );
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub async fn save_to_db(&self, state: &NodeState) -> Result<()> {
        state.kv_set("identity.seed", &self.seed).await?;
        state.kv_set("identity.pk", &self.public_key).await?;
        tracing::info!("saved identity to database");
        Ok(())
    }
}

fn derive_public_key(seed: &[u8; KEY_LEN]) -> [u8; KEY_LEN] {
    let mut hasher = Sha256::new();
    hasher.update(seed);
    let result = hasher.finalize();
    let mut pk = [0u8; KEY_LEN];
    pk.copy_from_slice(&result);
    pk
}
