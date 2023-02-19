use super::http::HTTPError;
use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use futures_util::future::LocalBoxFuture;
use slog::{error, Logger};
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

        debug!(logger, "request";
            "method" => ?req.method(),
            "path" => req.path(),
        );

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
                            error!(logger, "Error: {output}");
                        } else {
                            error!(logger, "Error: {err:?}");
                        }
                    }
                    Ok(res)
                }
                Err(err) => {
                    debug!(logger, "Error occurred: {}", err);
                    Err(err)
                }
            }
        })
    }
}
