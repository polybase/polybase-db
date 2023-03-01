use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) timeout: Duration,
    pub(crate) keep_alive: bool,
}

impl Config {
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(1),
            keep_alive: true,
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}
