use futures::future::try_join_all;
use plotters::prelude::*;
// use reqwest::Client;
use serde_json::json;
use statrs::statistics::Statistics;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

pub struct BenchResult {
    total_duration: f64,
    avg_duration: f64,
    variance_duration: f64,
    std_dev_duration: f64,
    throughput: f64,
    durations: Vec<f64>,
}

#[tokio::main]
async fn main() {
    let base_url = "http://localhost:8080";

    // client hangs on 100k on MacOS
    for n in [1, 10, 100, 1000] {
        let BenchResult {
            total_duration,
            avg_duration,
            variance_duration,
            std_dev_duration,
            throughput,
            ..
        } = create_records(n, base_url).await;

        println!("Took {}s to create {n} records", total_duration);
        println!("  Average duration: {} seconds", avg_duration);
        println!("  Variance deviation: {} seconds", variance_duration);
        println!("  Standard deviation: {} seconds", std_dev_duration);
        println!("  Throughput: {} requests per second", throughput);
    }
}

async fn create_records(n: u64, base_url: &str) -> BenchResult {
    let client = reqwest::Client::builder()
        // .http2_prior_knowledge()
        .build()
        .unwrap();

    let collection_id = create_collection(base_url).await;

    let mut handles = Vec::new();
    let collection_id = Arc::new(collection_id.replace('/', "%2F"));
    let base_url = Arc::new(base_url.to_string());
    let now = Arc::new(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );

    for i in 0..n {
        let client = client.clone();
        let base_url = Arc::clone(&base_url);
        let collection_id = Arc::clone(&collection_id);
        let now = Arc::clone(&now);

        let handle = tokio::spawn(async move {
            let json_body = json!({
                "args": [format!("{}", *now + i)],
            });

            let url = format!("{}/v0/collections/{}/records", &base_url, &collection_id);

            let start = Instant::now();
            // let response = client.get("http://localhost:8080").send().await.unwrap();
            let response = client.post(url).json(&json_body).send().await.unwrap();
            let duration = start.elapsed();

            // Make sure the request was successful
            assert!(response.status().is_success());

            duration.as_secs_f64()
        });
        handles.push(handle);
    }

    let all_start = std::time::Instant::now();
    let durations = try_join_all(handles).await.unwrap();
    let total_duration = all_start.elapsed();
    let varience = durations.clone().variance();

    BenchResult {
        total_duration: total_duration.as_secs_f64(),
        avg_duration: durations.clone().mean(),
        variance_duration: varience,
        std_dev_duration: varience.sqrt(),
        throughput: n as f64 / total_duration.as_secs_f64(),
        durations,
    }
}

// async fn send_with_success(req: reqwest::RequestBuilder) -> Result<(), Box<dyn std::error::Error>> {
//     match req.send().await {
//         Ok(x) if x.status().as_u16() < 300 => Ok(()),
//         Ok(x) => Err(format!(
//             "Unexpected status code: {}, body: {:?}",
//             x.status(),
//             x.text().await.unwrap()
//         )
//         .into()),
//         Err(e) => Err(e.into()),
//     }
// }

async fn create_collection(base_url: &str) -> String {
    let client = reqwest::Client::builder().build().unwrap();

    let random_id = rand::random::<u64>();
    let id = format!("polybase/bench/{}/SampleCollection", &random_id);
    let req = client
        .post(format!("{base_url}/v0/collections/Collection/records"))
        .json(&serde_json::json!({
            "args": [&id, r#"
                collection SampleCollection {
                    id: string;

                    constructor (id: string) {
                        this.id = id;
                    }
                }
                "#],
        }));

    req.send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();

    id
}

async fn generate_chart(name: String, durations: Vec<f64>, num_bins: usize) {
    let current_file = file!();
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let absolute_current_file = fs::canonicalize(project_root.join(current_file)).unwrap();
    let current_dir = absolute_current_file.parent().unwrap();

    let svg_path = current_dir.join(name);

    println!("Plotting histogram to {:?}", svg_path);

    // Plotting histogram
    let root = SVGBackend::new(svg_path.to_str().unwrap(), (640, 480)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    let max_duration = *durations
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap();
    let min_duration = *durations
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap();

    let mut chart = ChartBuilder::on(&root)
        // .caption("Histogram of Request Durations", ("", 50).into_font())
        .margin(5)
        .x_label_area_size(30)
        .y_label_area_size(30)
        .build_cartesian_2d(min_duration..max_duration, 0usize..400usize)
        .unwrap();

    chart.configure_mesh().draw().unwrap();

    let histogram = durations.iter().fold(vec![0; num_bins], |mut acc, &v| {
        let index =
            ((v - min_duration) / (max_duration - min_duration) * (num_bins as f64) - 1.0) as usize;
        acc[index] += 1;
        acc
    });

    chart
        .draw_series(histogram.into_iter().zip(0..).map(|(y, x)| {
            Rectangle::new(
                [
                    (
                        (x as f64 / num_bins as f64) * (max_duration - min_duration)
                            + min_duration
                            + 0.0001,
                        0,
                    ),
                    (
                        ((x + 1) as f64 / num_bins as f64) * (max_duration - min_duration)
                            + min_duration
                            - 0.0001,
                        y,
                    ),
                ],
                BLUE.filled(),
            )
        }))
        .unwrap();

    root.present().unwrap();
}
