use crate::api::ServerConfig;

#[tokio::test]
async fn test_start_stop() {
    let api_port;
    {
        let server = super::Server::setup_and_wait(Some(ServerConfig {
            keep_port_after_drop: true,
            ..Default::default()
        }))
        .await;
        api_port = server.api_port;
    }

    for i in 0..10 {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        if reqwest::get(format!("http://localhost:{api_port}/v0/health"))
            .await
            .is_ok()
        {
            if i == 9 {
                panic!("Server should be stopped");
            }
        } else {
            break;
        }
    }
}
