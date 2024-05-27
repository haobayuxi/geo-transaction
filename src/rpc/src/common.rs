#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadStruct {
    #[prost(uint64, tag = "1")]
    pub key: u64,
    #[prost(uint32, tag = "2")]
    pub table_id: u32,
    #[prost(string, optional, tag = "3")]
    pub value: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, optional, tag = "4")]
    pub timestamp: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Msg {
    #[prost(uint64, tag = "1")]
    pub txn_id: u64,
    #[prost(message, repeated, tag = "2")]
    pub read_set: ::prost::alloc::vec::Vec<ReadStruct>,
    #[prost(message, repeated, tag = "3")]
    pub write_set: ::prost::alloc::vec::Vec<ReadStruct>,
    #[prost(message, repeated, tag = "4")]
    pub insert: ::prost::alloc::vec::Vec<ReadStruct>,
    #[prost(message, repeated, tag = "5")]
    pub delete: ::prost::alloc::vec::Vec<ReadStruct>,
    #[prost(enumeration = "TxnOp", tag = "6")]
    pub op: i32,
    #[prost(bool, tag = "7")]
    pub success: bool,
    #[prost(bool, tag = "8")]
    pub read_only: bool,
    #[prost(uint64, optional, tag = "9")]
    pub ts: ::core::option::Option<u64>,
    #[prost(uint64, repeated, tag = "10")]
    pub deps: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Echo {
    #[prost(bool, tag = "1")]
    pub success: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Throughput {
    #[prost(uint64, tag = "1")]
    pub committed: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Latencies {
    #[prost(uint64, repeated, tag = "1")]
    pub latency: ::prost::alloc::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ts {
    #[prost(uint64, tag = "1")]
    pub ts: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TxnOp {
    Execute = 0,
    Validate = 1,
    Commit = 2,
    Abort = 3,
    Accept = 4,
}
#[doc = r" Generated client implementations."]
pub mod data_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct DataServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DataServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> DataServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn communication(
            &mut self,
            request: impl tonic::IntoRequest<super::Msg>,
        ) -> Result<tonic::Response<super::Msg>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/common.DataService/communication");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for DataServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for DataServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DataServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod cto_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct CtoServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CtoServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CtoServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn get_start_ts(
            &mut self,
            request: impl tonic::IntoRequest<super::Echo>,
        ) -> Result<tonic::Response<super::Ts>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/common.CtoService/get_start_ts");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn set_commit_ts(
            &mut self,
            request: impl tonic::IntoRequest<super::Ts>,
        ) -> Result<tonic::Response<super::Echo>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/common.CtoService/set_commit_ts");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for CtoServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for CtoServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CtoServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod throughput_statistics_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct ThroughputStatisticsServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ThroughputStatisticsServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ThroughputStatisticsServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::Echo>,
        ) -> Result<tonic::Response<super::Throughput>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/common.ThroughputStatisticsService/get");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for ThroughputStatisticsServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for ThroughputStatisticsServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ThroughputStatisticsServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod data_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with DataServiceServer."]
    #[async_trait]
    pub trait DataService: Send + Sync + 'static {
        async fn communication(
            &self,
            request: tonic::Request<super::Msg>,
        ) -> Result<tonic::Response<super::Msg>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct DataServiceServer<T: DataService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: DataService> DataServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for DataServiceServer<T>
    where
        T: DataService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/common.DataService/communication" => {
                    #[allow(non_camel_case_types)]
                    struct communicationSvc<T: DataService>(pub Arc<T>);
                    impl<T: DataService> tonic::server::UnaryService<super::Msg> for communicationSvc<T> {
                        type Response = super::Msg;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::Msg>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).communication(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = communicationSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: DataService> Clone for DataServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: DataService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: DataService> tonic::transport::NamedService for DataServiceServer<T> {
        const NAME: &'static str = "common.DataService";
    }
}
#[doc = r" Generated server implementations."]
pub mod cto_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with CtoServiceServer."]
    #[async_trait]
    pub trait CtoService: Send + Sync + 'static {
        async fn get_start_ts(
            &self,
            request: tonic::Request<super::Echo>,
        ) -> Result<tonic::Response<super::Ts>, tonic::Status>;
        async fn set_commit_ts(
            &self,
            request: tonic::Request<super::Ts>,
        ) -> Result<tonic::Response<super::Echo>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct CtoServiceServer<T: CtoService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: CtoService> CtoServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for CtoServiceServer<T>
    where
        T: CtoService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/common.CtoService/get_start_ts" => {
                    #[allow(non_camel_case_types)]
                    struct get_start_tsSvc<T: CtoService>(pub Arc<T>);
                    impl<T: CtoService> tonic::server::UnaryService<super::Echo> for get_start_tsSvc<T> {
                        type Response = super::Ts;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::Echo>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_start_ts(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = get_start_tsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/common.CtoService/set_commit_ts" => {
                    #[allow(non_camel_case_types)]
                    struct set_commit_tsSvc<T: CtoService>(pub Arc<T>);
                    impl<T: CtoService> tonic::server::UnaryService<super::Ts> for set_commit_tsSvc<T> {
                        type Response = super::Echo;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::Ts>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).set_commit_ts(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = set_commit_tsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: CtoService> Clone for CtoServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: CtoService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: CtoService> tonic::transport::NamedService for CtoServiceServer<T> {
        const NAME: &'static str = "common.CtoService";
    }
}
#[doc = r" Generated server implementations."]
pub mod throughput_statistics_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ThroughputStatisticsServiceServer."]
    #[async_trait]
    pub trait ThroughputStatisticsService: Send + Sync + 'static {
        async fn get(
            &self,
            request: tonic::Request<super::Echo>,
        ) -> Result<tonic::Response<super::Throughput>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ThroughputStatisticsServiceServer<T: ThroughputStatisticsService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: ThroughputStatisticsService> ThroughputStatisticsServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for ThroughputStatisticsServiceServer<T>
    where
        T: ThroughputStatisticsService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/common.ThroughputStatisticsService/get" => {
                    #[allow(non_camel_case_types)]
                    struct getSvc<T: ThroughputStatisticsService>(pub Arc<T>);
                    impl<T: ThroughputStatisticsService> tonic::server::UnaryService<super::Echo> for getSvc<T> {
                        type Response = super::Throughput;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::Echo>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = getSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: ThroughputStatisticsService> Clone for ThroughputStatisticsServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: ThroughputStatisticsService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ThroughputStatisticsService> tonic::transport::NamedService
        for ThroughputStatisticsServiceServer<T>
    {
        const NAME: &'static str = "common.ThroughputStatisticsService";
    }
}
