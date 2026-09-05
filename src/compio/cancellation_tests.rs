//! Controlled PostgreSQL peers exercising the Compio runtime without a database.
#![allow(
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::shadow_unrelated
)]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::oneshot;

use super::Pool;
use crate::compio::{Conn, stream::Stream};
use crate::{Error, Opts, Result};

const LIMIT: Duration = Duration::from_secs(5);

fn message(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![kind];
    bytes.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn payload(peer: &mut UnixStream) {
    let mut length = [0; 4];
    peer.read_exact(&mut length).unwrap();
    let mut body = vec![0; u32::from_be_bytes(length) as usize - 4];
    peer.read_exact(&mut body).unwrap();
}

fn respond(peer: &mut UnixStream, terminal: u8, response: &[u8]) {
    loop {
        let mut kind = [0];
        peer.read_exact(&mut kind).unwrap();
        payload(peer);
        if kind[0] == terminal {
            break;
        }
    }
    peer.write_all(response).unwrap();
}

async fn connection(
    script: impl FnOnce(&mut UnixStream) + Send + 'static,
) -> (Conn, JoinHandle<()>) {
    let (client, mut peer) = UnixStream::pair().unwrap();
    peer.set_read_timeout(Some(LIMIT)).unwrap();
    peer.set_write_timeout(Some(LIMIT)).unwrap();
    let server = std::thread::spawn(move || {
        payload(&mut peer); // StartupMessage has no type byte.
        let startup = [
            message(b'R', &0_u32.to_be_bytes()),
            message(b'K', &[0, 0, 0, 123, 0, 0, 0, 1]),
            message(b'Z', b"I"),
        ]
        .concat();
        peer.write_all(&startup).unwrap();
        script(&mut peer);
    });
    let opts = Opts {
        ssl_mode: crate::SslMode::Disable,
        upgrade_to_unix_socket: false,
        ..Opts::default()
    };
    let stream = Stream::unix(compio::net::UnixStream::from_std(client).unwrap());
    (Conn::new_with_stream(stream, opts).await.unwrap(), server)
}

#[compio::test]
async fn fatal_pipeline_errors_reject_continuation() {
    for sync in [false, true] {
        let (mut conn, server) = connection(move |peer| {
            let fatal = message(b'E', b"SFATAL\0VFATAL\0C57P01\0Mterminating connection\0\0");
            respond(peer, if sync { b'S' } else { b'H' }, &fatal);
        })
        .await;
        let mut pipeline = crate::compio::pipeline::Pipeline::new_inner(&mut conn);
        let ticket = pipeline.exec("SELECT 42", ()).unwrap();
        // With Sync, leave an execution before it in the expectation queue.
        // Without Sync, there is no ReadyForQuery to consume at all.
        let _second = pipeline.exec("SELECT 99", ()).unwrap();
        if sync {
            pipeline.sync().await.unwrap();
        } else {
            pipeline.flush().await.unwrap();
        }
        let error = pipeline.claim_drop(ticket).await.unwrap_err();
        assert!(matches!(error, Error::Server(_)));
        assert!(error.is_connection_broken());
        assert!(matches!(
            pipeline.exec("SELECT 101", ()),
            Err(Error::ConnectionBroken)
        ));
        assert!(matches!(
            pipeline.sync().await,
            Err(Error::ConnectionBroken)
        ));
        pipeline.cleanup().await;
        drop(pipeline);
        assert!(conn.is_broken());
        server.join().unwrap();
    }
}

#[compio::test]
#[expect(
    clippy::mem_forget,
    reason = "exercise interruption without running Drop"
)]
async fn forgotten_pipeline_cannot_complete_pool_reset() {
    let (done, wait_done) = std::sync::mpsc::channel();
    let (ready_sent, ready_received) = oneshot::channel();
    let (mut conn, server) = connection(move |peer| {
        let response = [
            message(b'1', &[]),
            message(b'2', &[]),
            message(b'n', &[]),
            message(b'C', b"SELECT 1\0"),
        ]
        .concat();
        respond(peer, b'H', &response);
        respond(peer, b'S', &message(b'Z', b"I"));
        ready_sent.send(()).unwrap();
        // Keep the peer alive without replying to DISCARD ALL.
        wait_done.recv_timeout(LIMIT).unwrap();
    })
    .await;
    let (sent, received) = oneshot::channel();
    let mut future = Box::pin(conn.pipeline(async |pipeline| {
        let ticket = pipeline.exec("SELECT 42", ())?;
        pipeline.flush().await?;
        pipeline.claim_drop(ticket).await?;
        pipeline.sync().await?;
        sent.send(()).unwrap();
        std::future::pending::<Result<()>>().await
    }));
    tokio::select! {
        biased;
        _ = &mut future => panic!("pipeline unexpectedly finished"),
        _ = received => {}
    }
    std::mem::forget(future);
    ready_received.await.unwrap();
    assert!(conn.is_broken());
    let pool = Pool::new(Opts::default());
    pool.check_in(conn).await;
    done.send(()).unwrap();
    assert!(pool.conns.borrow().is_empty());
    server.join().unwrap();
}

#[compio::test]
#[expect(
    clippy::mem_forget,
    reason = "exercise interruption without running Drop"
)]
async fn abandoned_portal_callbacks_prevent_reuse() {
    for forget in [false, true] {
        let (mut conn, server) = connection(|peer| {
            let bind = [message(b'1', &[]), message(b'2', &[])].concat();
            respond(peer, b'H', &bind);
            let complete = [message(b'n', &[]), message(b'C', b"UPDATE 1\0")].concat();
            respond(peer, b'H', &complete);
        })
        .await;
        let (executed, received) = oneshot::channel();
        let mut future =
            Box::pin(
                conn.exec_portal("UPDATE t SET value = 42", (), async |portal| {
                    let mut handler = crate::handler::DropHandler::new();
                    portal.exec(0, &mut handler).await?;
                    executed.send(()).unwrap();
                    std::future::pending::<Result<()>>().await
                }),
            );
        tokio::select! {
            biased;
            _ = &mut future => panic!("portal unexpectedly finished"),
            _ = received => {}
        }
        if forget {
            std::mem::forget(future);
        } else {
            drop(future);
        }
        assert!(conn.is_broken());
        assert!(matches!(conn.ping().await, Err(Error::ConnectionBroken)));
        assert!(matches!(
            conn.lowlevel_sync().await,
            Err(Error::ConnectionBroken)
        ));
        let mut replacement = crate::compio::pipeline::Pipeline::new_inner(&mut conn);
        assert!(matches!(
            replacement.sync().await,
            Err(Error::ConnectionBroken)
        ));
        replacement.cleanup().await;
        server.join().unwrap();
    }
}

#[compio::test]
async fn completed_portal_scopes_preserve_reuse() {
    for callback_error in [false, true] {
        let (mut conn, server) = connection(|peer| {
            let bind = [message(b'1', &[]), message(b'2', &[])].concat();
            respond(peer, b'H', &bind);
            let suspended = [message(b'n', &[]), message(b's', &[])].concat();
            respond(peer, b'H', &suspended);
            let complete = [message(b'n', &[]), message(b'C', b"SELECT 1\0")].concat();
            respond(peer, b'H', &complete);
            respond(peer, b'S', &message(b'Z', b"I"));
            respond(
                peer,
                b'Q',
                &[message(b'I', &[]), message(b'Z', b"I")].concat(),
            );
        })
        .await;
        let result = conn
            .exec_portal("SELECT 42", (), async |portal| {
                let mut handler = crate::handler::DropHandler::new();
                assert!(portal.exec(1, &mut handler).await?);
                assert!(!portal.exec(0, &mut handler).await?);
                if callback_error {
                    Err(Error::InvalidUsage("callback stopped after fetch".into()))
                } else {
                    Ok(())
                }
            })
            .await;
        assert_eq!(result.is_err(), callback_error);
        assert!(!conn.is_broken());
        conn.ping().await.unwrap();
        server.join().unwrap();
    }
}
