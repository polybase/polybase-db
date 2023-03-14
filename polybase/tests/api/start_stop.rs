#[tokio::test]
async fn test_start_stop() {
    let api_port;
    {
        let server = super::Server::setup_and_wait().await;
        api_port = server.api_port;
    }

    reqwest::get(format!("http://localhost:{api_port}/v0/health"))
        .await
        .expect_err("Server should be stopped");
}
