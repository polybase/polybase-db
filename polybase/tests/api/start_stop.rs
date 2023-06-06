#[tokio::test]
async fn test_start_stop() {
    let api_port;
    {
        let server = super::Server::setup_and_wait(None).await;
        api_port = server.api_port;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    reqwest::get(format!("http://localhost:{api_port}/v0/health"))
        .await
        .expect_err("Server should be stopped");
}
