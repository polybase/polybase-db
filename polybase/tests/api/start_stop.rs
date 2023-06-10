use tokio::time;

#[tokio::test]
async fn test_start_stop() {
    let api_port;
    {
        let server = super::Server::setup_and_wait(None).await;
        api_port = server.api_port;
    }

    let _ = time::timeout(
        time::Duration::from_secs(3),
        wait_for_server_port_to_be_released(api_port),
    )
    .await;

    reqwest::get(format!("http://localhost:{api_port}/v0/health"))
        .await
        .expect_err("Server should be stopped");
}

async fn wait_for_server_port_to_be_released(port: u16) {
    use std::net::Ipv4Addr;
    use tokio::net::TcpListener;

    while TcpListener::bind((Ipv4Addr::UNSPECIFIED, port))
        .await
        .is_err()
    {
        time::sleep(time::Duration::from_millis(100)).await;
    }
}
