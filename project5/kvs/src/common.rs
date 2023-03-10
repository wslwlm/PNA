use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String } 
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Get(Option<String>),
    Set,
    Remove,
    Err(String),
}