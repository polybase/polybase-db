use super::http::HTTPError;
use super::metrics::MetricsData;
use super::reason::ReasonCode;
use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use futures_util::future::LocalBoxFuture;
use slog::{crit, error, info, Logger};
use std::future::{ready, Ready};

pub struct SlogMiddleware {
    logger: Logger,
}

impl SlogMiddleware {
    pub fn new(logger: Logger) -> Self {
        Self { logger }
    }
}

impl<S, B> Transform<S, ServiceRequest> for SlogMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = SlogMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(SlogMiddlewareService {
            service,
            logger: self.logger.clone(),
        }))
    }
}

pub struct SlogMiddlewareService<S> {
    service: S,
    logger: Logger,
}

impl<S, B> Service<ServiceRequest> for SlogMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let logger = self.logger.clone();

        let fut = self.service.call(req);

        Box::pin(async move {
            match fut.await {
                Ok(res) => {
                    if let Some(err) = res.response().error() {
                        if let Some(err) = err.as_error::<HTTPError>() {
                            let mut output: String = format!("{err}");
                            // Log out each error
                            let mut error: &dyn std::error::Error = err;
                            while let Some(source) = error.source() {
                                output = format!("{output}\n  Caused by: {source}");
                                error = source;
                            }
                            if err.reason == ReasonCode::Internal {
                                let mut e = sentry::event_from_error(err);
                                // Reverse the errors (Sentry seems to have a bug)
                                e.exception.values.reverse();
                                sentry::capture_event(e);
                                crit!(logger, "Error: {output}");
                            } else {
                                error!(logger, "Error: {output}");
                            }
                        } else {
                            crit!(logger, "Error: {err:?}");
                        }
                    }

                    // log any metrics data that might be available
                    {
                        if let Some(metrics_data) = res.response().extensions().get::<MetricsData>()
                        {
                            info!(logger, "{}", metrics_data; metrics_data);
                        }
                    }
                    Ok(res)
                }
                Err(err) => {
                    error!(logger, "Error occurred: {}", err);
                    Err(err)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{get, test, App, HttpResponse, Responder};
    use slog::{o, Logger};

    fn dummy_logger() -> Logger {
        let drain = slog::Discard;
        let logger = slog::Logger::root(drain, o!());
        logger
    }

    #[get("/num-records")]
    async fn dummy_num_recs_returned_handler() -> impl Responder {
        let mut resp = HttpResponse::Ok().body("");
        resp.extensions_mut()
            .insert(MetricsData::NumberOfRecordsBeingReturned {
                req_uri: "/v0/collections/Collection/records".to_string(),
                num_records: 11,
            });
        resp
    }

    #[actix_web::test]
    async fn test_number_of_records_being_returned_metrics_data() {
        let app = App::new()
            .wrap(SlogMiddleware {
                logger: dummy_logger(),
            })
            .service(dummy_num_recs_returned_handler);
        let app = test::init_service(app).await;

        let req = test::TestRequest::get().uri("/num-records").to_request();
        let resp = test::call_service(&app, req).await;

        assert!(resp.status().is_success());

        let resp = resp.response().extensions();

        let metrics_data = resp.get::<MetricsData>().unwrap();

        match metrics_data {
            MetricsData::NumberOfRecordsBeingReturned {
                req_uri,
                num_records,
            } => {
                assert_eq!("/v0/collections/Collection/records", req_uri);
                assert_eq!(11, *num_records);
            }
        }
    }
}
