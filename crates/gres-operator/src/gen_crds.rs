//! `gen-crds` subcommand: writes the CRD manifests this operator owns.

use std::{fs, path::Path};

use crate::crd::{Gres, GresTenant};

/// Writes every CRD that this operator owns into `out_dir` as
/// `<group>_<plural>.yaml`. This function overwrites an existing file.
///
/// # Errors
///
/// Returns an error when the directory cannot be created, a CRD cannot be
/// serialized, or a file cannot be written.
pub fn write_all(out_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_one::<Gres>(out_dir)?;
    write_one::<GresTenant>(out_dir)?;
    Ok(())
}

fn write_one<K>(out_dir: &Path) -> anyhow::Result<()>
where
    K: kube::Resource<DynamicType = ()> + kube::CustomResourceExt,
{
    let crd = K::crd();
    let group = &crd.spec.group;
    let plural = &crd.spec.names.plural;
    let file = out_dir.join(format!("{group}_{plural}.yaml"));
    let yaml = serde_yaml::to_string(&crd)?;
    fs::write(&file, yaml)?;
    eprintln!("wrote {}", file.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    use kube::CustomResourceExt as _;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn writes_gres_and_gres_tenant_crd_files() {
        let dir = tempdir().unwrap();
        write_all(dir.path()).unwrap();
        for (file, expected) in [
            ("krabka.io_greses.yaml", Gres::crd()),
            ("krabka.io_grestenants.yaml", GresTenant::crd()),
        ] {
            let path = dir.path().join(file);
            let yaml = std::fs::read_to_string(&path).unwrap();
            let written: CustomResourceDefinition = serde_yaml::from_str(&yaml).unwrap();
            assert!(written == expected, "case {file:?}");
        }
        assert!(std::fs::read_dir(dir.path()).unwrap().count() == 2);
    }
}
