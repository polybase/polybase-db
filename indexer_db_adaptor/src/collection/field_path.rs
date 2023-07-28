use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct FieldPath(pub(crate) Vec<String>);

impl From<Vec<Cow<'_, str>>> for FieldPath {
    fn from(v: Vec<Cow<'_, str>>) -> Self {
        Self(v.into_iter().map(|s| s.into_owned()).collect())
    }
}

impl PartialEq<&[&str]> for FieldPath {
    fn eq(&self, other: &&[&str]) -> bool {
        self.0.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<'de> Deserialize<'de> for FieldPath {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = Cow::<'de, str>::deserialize(deserializer)?;
        let mut path = Vec::new();
        for part in s.split('.') {
            path.push(part.to_string());
        }
        Ok(FieldPath(path))
    }
}

impl Serialize for FieldPath {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = String::new();
        for (i, part) in self.0.iter().enumerate() {
            if i > 0 {
                s.push('.');
            }
            s.push_str(part);
        }
        serializer.serialize_str(&s)
    }
}
