//! Connection helper.
use tokio::net::TcpStream;

use tungstenite::{
    error::{Error, UrlError},
    handshake::client::{Request, Response},
    protocol::WebSocketConfig,
};

use crate::{domain, stream::MaybeTlsStream, Connector, IntoClientRequest, WebSocketStream};

use bluer::{
    rfcomm::{SocketAddr as BtSocketAddr, Stream as BtStream},
    Address as BtAddress,
};
use std::str::FromStr;

use tokio::io::{AsyncRead, AsyncWrite};

pub trait BoxedStreamTrait: AsyncRead + AsyncWrite + Unpin {}

impl BoxedStreamTrait for TcpStream {}

impl BoxedStreamTrait for BtStream {}

pub type BoxedStream = Box<dyn BoxedStreamTrait + Send + Sync>;

/// Connect to a given URL.
pub async fn connect_async<R>(
    request: R,
) -> Result<(WebSocketStream<MaybeTlsStream<BoxedStream>>, Response), Error>
where
    R: IntoClientRequest + Unpin,
{
    connect_async_with_config(request, None, false).await
}

/// The same as `connect_async()` but the one can specify a websocket configuration.
/// Please refer to `connect_async()` for more details. `disable_nagle` specifies if
/// the Nagle's algorithm must be disabled, i.e. `set_nodelay(true)`. If you don't know
/// what the Nagle's algorithm is, better leave it set to `false`.
pub async fn connect_async_with_config<R>(
    request: R,
    config: Option<WebSocketConfig>,
    disable_nagle: bool,
) -> Result<(WebSocketStream<MaybeTlsStream<BoxedStream>>, Response), Error>
where
    R: IntoClientRequest + Unpin,
{
    connect(request.into_client_request()?, config, disable_nagle, None).await
}

/// The same as `connect_async()` but the one can specify a websocket configuration,
/// and a TLS connector to use. Please refer to `connect_async()` for more details.
/// `disable_nagle` specifies if the Nagle's algorithm must be disabled, i.e.
/// `set_nodelay(true)`. If you don't know what the Nagle's algorithm is, better
/// leave it to `false`.
#[cfg(any(feature = "native-tls", feature = "__rustls-tls"))]
pub async fn connect_async_tls_with_config<R>(
    request: R,
    config: Option<WebSocketConfig>,
    disable_nagle: bool,
    connector: Option<Connector>,
) -> Result<(WebSocketStream<MaybeTlsStream<BoxedStream>>, Response), Error>
where
    R: IntoClientRequest + Unpin,
{
    connect(request.into_client_request()?, config, disable_nagle, connector).await
}

async fn connect(
    request: Request,
    config: Option<WebSocketConfig>,
    disable_nagle: bool,
    connector: Option<Connector>,
) -> Result<(WebSocketStream<MaybeTlsStream<BoxedStream>>, Response), Error> {
    let domain = domain(&request)?;

    if domain.ends_with(".bt") {
        // Handle Bluetooth connection
        let mac = domain
            .replace(".bt", "")
            .to_uppercase()
            .chars()
            .collect::<Vec<_>>()
            .chunks(2)
            .map(|chunk| chunk.iter().collect::<String>())
            .collect::<Vec<_>>()
            .join(":");

        let addr =
            BtAddress::from_str(&mac).map_err(|_| Error::Url(UrlError::UnsupportedUrlScheme))?;
        let target_sa = BtSocketAddr::new(addr, 4);
        let stream = BtStream::connect(target_sa).await.map_err(|e| {
            Error::Io(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))
        })?;

        let boxed_stream: BoxedStream = Box::new(stream);
        crate::tls::client_async_tls_with_config(request, boxed_stream, config, connector).await
    } else {
        // Handle TCP connection
        let port = request
            .uri()
            .port_u16()
            .or_else(|| match request.uri().scheme_str() {
                Some("wss") => Some(443),
                Some("ws") => Some(80),
                _ => None,
            })
            .ok_or(Error::Url(UrlError::UnsupportedUrlScheme))?;

        let addr = format!("{domain}:{port}");
        let socket = TcpStream::connect(addr).await.map_err(Error::Io)?;

        if disable_nagle {
            socket.set_nodelay(true)?;
        }

        let boxed_stream: BoxedStream = Box::new(socket);
        crate::tls::client_async_tls_with_config(request, boxed_stream, config, connector).await
    }
}
