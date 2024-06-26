use reqwest::header;
use serde_json::Value;
use std::collections::HashMap;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct Headers {
    pub headers: HashMap<String, String>,
}

impl Headers {
    pub fn new(headers: HashMap<String, String>) -> Self {
        Headers { headers }
    }

    pub fn add_header(&mut self, key: &str, value: &str) {
        self.headers.insert(key.to_string(), value.to_string());
    }
}

#[async_trait]
pub trait DaprInvoker {
    fn new(dapr_host: String, dapr_port: u16, app_id: String, method: String) -> Self;
    async fn invoke(&self, data: Value, headers: Headers) -> Result<(), Box<dyn std::error::Error>>;
}

#[async_trait]
pub trait DaprPublisher {
    fn new(dapr_host: String, dapr_port: u16, pubsub: String, topic: String) -> Self;
    async fn publish(&self, data: Value, headers: Headers) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct DaprHttpPublisher{
    client: reqwest::Client,
    dapr_host: String,
    dapr_port: u16,
    pubsub: String,
    topic: String,
}

#[async_trait]
impl DaprPublisher for DaprHttpPublisher {
    fn new(dapr_host: String, dapr_port: u16, pubsub: String, topic: String) -> Self {
        DaprHttpPublisher {
            client: reqwest::Client::new(),
            dapr_host,
            dapr_port,
            pubsub,
            topic,
        }
    }

    async fn publish(&self, data: Value, headers: Headers) -> Result<(), Box<dyn std::error::Error>> {
        let mut request  = self.client
            .post(format!(
                "http://{}:{}/v1.0/publish/{}/{}",
                self.dapr_host, self.dapr_port, self.pubsub, self.topic
            ))
            .json(&data);

        for (key, value) in headers.headers.iter() {
            request = request.header(key, value);
        }

        let response = request.send().await;

        match response {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }
}


pub struct DaprHttpInvoker {
    client: reqwest::Client,
    dapr_host: String,
    dapr_port: u16,
    app_id: String,
    method: String,
}

#[async_trait]
impl DaprInvoker for DaprHttpInvoker {
    fn new(dapr_host: String, dapr_port: u16, app_id: String, method: String) -> Self {
        DaprHttpInvoker {
            client: reqwest::Client::new(),
            dapr_host,
            dapr_port,
            app_id,
            method,
        }
    }

    async fn invoke(&self, data: Value, headers: Headers) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("http://{}:{}/{}", self.dapr_host, self.dapr_port,self.method);
        
        let mut request_headers = reqwest::header::HeaderMap::new();
        let headers = headers.headers.clone();
        for (key, value) in headers.iter() {
            request_headers.insert(key.parse::<reqwest::header::HeaderName>().unwrap(), value.parse().unwrap());
        }

        if !request_headers.contains_key("Content-Type") {
            request_headers.insert("Content-Type", "application/json".parse().unwrap());
        }

        request_headers.insert("dapr-app-id", self.app_id.parse().unwrap());

        let response = self
            .client
            .post(url)
            .headers(request_headers)
            .json(&data)
            .send()
            .await;

        match response {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }
}