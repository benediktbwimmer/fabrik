use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use fabrik_workflow::CompiledWorkflowArtifact;

fn main() -> Result<()> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: rehash-artifact <artifact.json>")?;
    let bytes = fs::read(&path)
        .with_context(|| format!("failed to read compiled artifact {}", path.display()))?;
    let mut artifact: CompiledWorkflowArtifact =
        serde_json::from_slice(&bytes).context("failed to decode compiled artifact JSON")?;
    artifact.artifact_hash = artifact.hash();
    fs::write(
        &path,
        serde_json::to_vec_pretty(&artifact)
            .context("failed to serialize rehashed compiled artifact")?,
    )
    .with_context(|| format!("failed to write compiled artifact {}", path.display()))?;
    Ok(())
}
