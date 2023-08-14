use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::string::ParseError;
use std::{borrow::Cow, fmt::Display};

// TODO: rename to PropertyPath
// TODO: Make turple private
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct FieldPath(pub Vec<String>);

impl FieldPath {
    pub fn new(path: Vec<String>) -> Self {
        Self(path)
    }

    pub fn new_single(path: String) -> Self {
        Self(vec![path])
    }

    /// Path to the id field, same for all records
    pub fn id() -> Self {
        Self(vec!["id".to_string()])
    }

    pub fn is_id(&self) -> bool {
        self.0.len() == 1 && self.0[0] == "id"
    }

    /// Name of the field, i.e. the last part of the path
    pub fn name(&self) -> &str {
        #[allow(clippy::expect_used)]
        self.0.last().expect("FieldPath is empty")
    }

    pub fn parent(&self) -> FieldPath {
        Self(self.0[..self.0.len() - 1].to_vec())
    }

    pub fn path(&self) -> String {
        self.0.join(".")
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn first(&self) -> Option<&String> {
        self.0.first()
    }

    pub fn as_slice(&self) -> &[String] {
        self.0.as_slice()
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|s| s.as_str())
    }

    /// Extends the path with the given path
    pub fn append(&self, path: String) -> Self {
        let mut new_path = self.0.clone();
        new_path.push(path);
        Self(new_path)
    }
}

impl From<&str> for FieldPath {
    fn from(v: &str) -> Self {
        Self(v.split('.').map(|s| s.to_string()).collect())
    }
}

impl FromStr for FieldPath {
    type Err = ParseError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(path))
    }
}

impl FromIterator<String> for FieldPath {
    fn from_iter<I: IntoIterator<Item = String>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Cow<'_, str>>> for FieldPath {
    fn from(v: Vec<Cow<'_, str>>) -> Self {
        Self(v.into_iter().map(|s| s.into_owned()).collect())
    }
}

impl From<Vec<String>> for FieldPath {
    fn from(v: Vec<String>) -> Self {
        Self(v)
    }
}

impl From<Vec<&String>> for FieldPath {
    fn from(v: Vec<&String>) -> Self {
        Self(v.iter().map(|s| s.to_string()).collect())
    }
}

impl From<Vec<&str>> for FieldPath {
    fn from(v: Vec<&str>) -> Self {
        Self(v.iter().map(|s| s.to_string()).collect())
    }
}

impl Display for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.join(".").fmt(f)
    }
}

impl From<FieldPath> for String {
    fn from(path: FieldPath) -> Self {
        path.path()
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
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_from_str() {
        let path = "foo.bar.baz";

        let field_path = FieldPath::from_str(path).unwrap();
        assert_eq!(
            field_path,
            FieldPath(vec![
                "foo".to_string(),
                "bar".to_string(),
                "baz".to_string()
            ])
        );

        let field_path = FieldPath::from(path);
        assert_eq!(
            field_path,
            FieldPath(vec![
                "foo".to_string(),
                "bar".to_string(),
                "baz".to_string()
            ])
        );
    }
}
