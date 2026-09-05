//! Controlled PostgreSQL peer: no database, TCP listener, credentials, or timing sleeps.
#![allow(
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::unwrap_used,
    clippy::unwrap_in_result,
    clippy::shadow_unrelated
)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::Pool;
use crate::handler::{ExtendedHandler, SimpleHandler};
use crate::protocol::backend::{CommandComplete, DataRow, RowDescription};
use crate::tokio::{Conn, stream::Stream};
use crate::{Opts, Result};

const LIMIT: Duration = Duration::from_secs(5);

fn message(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![kind];
    bytes.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

async fn read_message(peer: &mut UnixStream) -> (u8, Vec<u8>) {
    let kind = peer.read_u8().await.unwrap();
    let length = peer.read_u32().await.unwrap();
    let mut payload = vec![0; length as usize - 4];
    peer.read_exact(&mut payload).await.unwrap();
    (kind, payload)
}

async fn connection() -> (Conn, UnixStream) {
    let (client, mut peer) = UnixStream::pair().unwrap();
    let opts = Opts {
        // This peer speaks plaintext PostgreSQL, including in all-feature builds.
        ssl_mode: crate::SslMode::Disable,
        upgrade_to_unix_socket: false,
        ..Opts::default()
    };
    let server = async {
        let length = peer.read_u32().await.unwrap();
        let mut startup = vec![0; length as usize - 4];
        peer.read_exact(&mut startup).await.unwrap();
        peer.write_all(&message(b'R', &0_u32.to_be_bytes()))
            .await
            .unwrap();
        peer.write_all(&message(b'K', &[0, 0, 0, 123, 0, 0, 0, 1]))
            .await
            .unwrap();
        peer.write_all(&message(b'Z', b"I")).await.unwrap();
    };
    let (conn, ()) = tokio::join!(Conn::new_with_stream(Stream::unix(client), opts), server);
    (conn.unwrap(), peer)
}

struct Completed(Option<oneshot::Sender<()>>);
impl SimpleHandler for Completed {
    fn row(&mut self, _: RowDescription<'_>, _: DataRow<'_>) -> Result<()> {
        Ok(())
    }
    fn result_end(&mut self, _: CommandComplete<'_>) -> Result<()> {
        self.0.take().unwrap().send(()).unwrap();
        Ok(())
    }
}
impl ExtendedHandler for Completed {
    fn row(&mut self, _: RowDescription<'_>, _: DataRow<'_>) -> Result<()> {
        Ok(())
    }
    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        SimpleHandler::result_end(self, complete)
    }
}

// The peer forwards everything through CommandComplete and withholds only the
// final ReadyForQuery. The handler proves the client consumed CommandComplete
// before we drop the future; cancellation does not depend on scheduler timing.
async fn cancel_at_ready(conn: &mut Conn, peer: &mut UnixStream, extended: bool) {
    let (completed, received) = oneshot::channel();
    let mut handler = Completed(Some(completed));
    let query = async {
        if extended {
            conn.exec(
                "UPDATE t SET value = $1 RETURNING value",
                (7_i32,),
                &mut handler,
            )
            .await
        } else {
            conn.query("DO $$ BEGIN PERFORM pg_sleep(0.3); END $$", &mut handler)
                .await
        }
    };
    let server = async {
        loop {
            let (kind, _) = read_message(peer).await;
            if kind == if extended { b'S' } else { b'Q' } {
                break;
            }
        }
        if extended {
            peer.write_all(&message(b'1', &[])).await.unwrap();
            peer.write_all(&message(b'2', &[])).await.unwrap();
            peer.write_all(&row_data(7, true)).await.unwrap();
        }
        peer.write_all(&message(
            b'C',
            if extended { b"UPDATE 1\0" } else { b"DO\0" },
        ))
        .await
        .unwrap();
        received.await.unwrap();
    };
    tokio::select! {
        biased;
        result = query => panic!("query completed before ReadyForQuery: {result:?}"),
        () = server => {}
    }
}

async fn cancelled_check_in(extended: bool) {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        cancel_at_ready(&mut conn, &mut peer, extended).await;
        println!(
            "extended={extended}: after cancellation is_broken={}",
            conn.is_broken()
        );
        peer.write_all(&message(b'Z', b"I")).await.unwrap();
        let pool = Arc::new(Pool::new(Opts::default()));
        pool.check_in(conn).await;
        println!(
            "extended={extended}: idle connections after check-in={}",
            pool.conns.len()
        );
        assert!(
            pool.conns.is_empty(),
            "an abandoned ReadyForQuery must not complete DISCARD ALL"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn simple_cancellation_discards_connection() {
    cancelled_check_in(false).await;
}

#[tokio::test]
async fn extended_cancellation_discards_connection() {
    cancelled_check_in(true).await;
}

async fn respond(peer: &mut UnixStream, terminal: u8, response: &[u8]) {
    loop {
        if read_message(peer).await.0 == terminal {
            break;
        }
    }
    peer.write_all(response).await.unwrap();
}

fn row_data(value: i32, binary: bool) -> Vec<u8> {
    let mut columns = vec![0, 1];
    columns.extend_from_slice(b"value\0");
    columns.extend_from_slice(&[0; 6]); // table oid, attribute
    columns.extend_from_slice(&23_u32.to_be_bytes()); // int4
    columns.extend_from_slice(&4_i16.to_be_bytes());
    columns.extend_from_slice(&(-1_i32).to_be_bytes());
    columns.extend_from_slice(&i16::from(binary).to_be_bytes());
    let value = if binary {
        value.to_be_bytes().to_vec()
    } else {
        value.to_string().into_bytes()
    };
    let mut data = vec![0, 1];
    data.extend_from_slice(&(value.len() as u32).to_be_bytes());
    data.extend_from_slice(&value);
    [message(b'T', &columns), message(b'D', &data)].concat()
}

fn rows(value: i32, binary: bool) -> Vec<u8> {
    [row_data(value, binary), message(b'C', b"SELECT 1\0")].concat()
}

#[tokio::test]
async fn healthy_pool_reuse_and_distinct_results() {
    timeout(LIMIT, async {
        let (conn, mut peer) = connection().await;
        let pid = conn.connection_id();
        let pool = Arc::new(Pool::new(Opts::default()));
        let reset = [message(b'C', b"DISCARD ALL\0"), message(b'Z', b"I")].concat();
        let ((), ()) = tokio::join!(pool.check_in(conn), respond(&mut peer, b'Q', &reset));
        let ping = [message(b'I', &[]), message(b'Z', b"I")].concat();
        let (borrowed, ()) = tokio::join!(pool.get(), respond(&mut peer, b'Q', &ping));
        let mut borrowed = borrowed.unwrap();
        assert_eq!(borrowed.connection_id(), pid);
        for value in [42, 99] {
            let response = [rows(value, false), message(b'Z', b"I")].concat();
            let sql = format!("SELECT {value}");
            let (result, ()) = tokio::join!(
                borrowed.query_collect::<(i32,)>(&sql),
                respond(&mut peer, b'Q', &response)
            );
            assert_eq!(result.unwrap(), vec![(value,)]);
        }
        drop(borrowed);
        respond(&mut peer, b'Q', &reset).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pooled_drop_closes_without_cleanup_query() {
    timeout(LIMIT, async {
        let (conn, mut peer) = connection().await;
        let pool = Arc::new(Pool::new(Opts::default()));
        let mut borrowed = super::PooledConn {
            pool: Arc::clone(&pool),
            conn: std::mem::ManuallyDrop::new(conn),
            _permit: None,
        };
        cancel_at_ready(&mut borrowed, &mut peer, true).await;
        drop(borrowed);
        let mut byte = [0];
        assert_eq!(
            peer.read(&mut byte).await.unwrap(),
            0,
            "check-in must close without sending DISCARD ALL"
        );
        assert!(pool.conns.is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retained_connection_rejects_all_entry_points() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        cancel_at_ready(&mut conn, &mut peer, false).await;
        let stmt = crate::PreparedStatement {
            idx: 0,
            param_oids: vec![],
            row_desc_payload: None,
        };
        let mut handler = crate::handler::DropHandler::new();
        assert!(matches!(
            conn.query_drop("SELECT 42").await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.exec_drop("SELECT $1", (42,)).await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.prepare("SELECT 42").await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.prepare_batch(&[]).await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.exec_batch("SELECT $1", &[(42,)]).await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.close_statement(&stmt).await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.lowlevel_bind("", "", ()).await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.lowlevel_execute("", 0, &mut handler).await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.lowlevel_close_portal("").await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.lowlevel_flush().await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.lowlevel_sync().await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.exec_portal("SELECT 42", (), async |_| Ok(())).await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.pipeline(async |_| Ok(())).await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unpolled_future_does_not_poison_connection() {
    timeout(LIMIT, async {
        let (mut conn, peer) = connection().await;
        drop(conn.query_drop("SELECT 42"));
        drop(conn.exec_drop("SELECT $1", (42,)));
        drop(conn.prepare("SELECT 42"));
        drop(conn.pipeline(async |_| Ok(())));
        let mut pipeline = crate::tokio::pipeline::Pipeline::new_inner(&mut conn);
        let _ticket = pipeline.exec("SELECT 42", ()).unwrap();
        drop(pipeline.sync());
        drop(pipeline);
        assert!(!conn.is_broken());
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn consumed_sql_errors_preserve_reuse() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        let error = [
            message(b'E', b"SERROR\0C22012\0Mdivision by zero\0\0"),
            message(b'Z', b"I"),
        ]
        .concat();
        let (result, ()) = tokio::join!(
            conn.query_drop("SELECT 1/0"),
            respond(&mut peer, b'Q', &error)
        );
        assert!(matches!(result, Err(crate::Error::Server(_))));
        assert!(!conn.is_broken());
        let (result, ()) = tokio::join!(
            conn.exec_drop("SELECT 1/$1", (0,)),
            respond(&mut peer, b'S', &error)
        );
        assert!(matches!(result, Err(crate::Error::Server(_))));
        assert!(!conn.is_broken());
        let (result, ()) = tokio::join!(
            conn.exec_batch("SELECT 1/$1", &[(0,)]),
            respond(&mut peer, b'S', &error)
        );
        assert!(matches!(result, Err(crate::Error::Server(_))));
        assert!(!conn.is_broken());
        let response = [rows(99, false), message(b'Z', b"I")].concat();
        let (result, ()) = tokio::join!(
            conn.query_collect::<(i32,)>("SELECT 99"),
            respond(&mut peer, b'Q', &response)
        );
        assert_eq!(result.unwrap(), vec![(99,)]);
    })
    .await
    .unwrap();
}

// Cancel after the peer has received the request, before any response. This
// exercises each independent driver; no server timer or sleep is involved.
async fn cancel_request<T>(
    future: impl std::future::Future<Output = Result<T>>,
    peer: &mut UnixStream,
    terminal: u8,
) {
    tokio::select! {
        biased;
        _ = future => panic!("operation finished without a response"),
        () = respond(peer, terminal, &[]) => {}
    }
}

#[tokio::test]
async fn cancellation_covers_prepare_batches_and_portals() {
    timeout(LIMIT, async {
        for operation in 0..10 {
            let (mut conn, mut peer) = connection().await;
            let stmt = crate::PreparedStatement {
                idx: 0,
                param_oids: vec![],
                row_desc_payload: None,
            };
            let mut handler = crate::handler::DropHandler::new();
            match operation {
                0 => cancel_request(conn.prepare("SELECT 42"), &mut peer, b'S').await,
                1 => cancel_request(conn.prepare_batch(&["SELECT 42"]), &mut peer, b'S').await,
                2 => cancel_request(conn.exec_batch("SELECT $1", &[(42,)]), &mut peer, b'S').await,
                3 => cancel_request(conn.close_statement(&stmt), &mut peer, b'S').await,
                4 => cancel_request(conn.lowlevel_bind("", "", ()), &mut peer, b'H').await,
                5 => {
                    cancel_request(conn.lowlevel_execute("", 0, &mut handler), &mut peer, b'H')
                        .await
                }
                6 => cancel_request(conn.lowlevel_close_portal(""), &mut peer, b'H').await,
                7 => cancel_request(conn.lowlevel_sync(), &mut peer, b'S').await,
                8 => {
                    cancel_request(
                        conn.exec_portal("SELECT 42", (), async |_| Ok(())),
                        &mut peer,
                        b'H',
                    )
                    .await
                }
                9 => {
                    cancel_request(
                        conn.create_named_portal("p", &"SELECT 42", &()),
                        &mut peer,
                        b'H',
                    )
                    .await
                }
                _ => panic!("unknown test case"),
            }
            assert!(conn.is_broken(), "operation {operation}");
            assert!(matches!(
                conn.ping().await,
                Err(crate::Error::ConnectionBroken)
            ));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pipeline_cancellation_and_drop_cannot_lose_expectations() {
    timeout(LIMIT, async {
        for operation in 0..3 {
            let (mut conn, mut peer) = connection().await;
            let mut pipeline = crate::tokio::pipeline::Pipeline::new_inner(&mut conn);
            let ticket = pipeline.exec("SELECT $1", (42,)).unwrap();
            pipeline.sync().await.unwrap();
            match operation {
                0 => cancel_request(pipeline.claim_drop(ticket), &mut peer, b'S').await,
                1 => {
                    tokio::select! {
        biased;
                        () = pipeline.cleanup() => panic!("cleanup finished without responses"),
                        () = respond(&mut peer, b'S', &[]) => {}
                    }
                }
                2 => {} // Drop between completed calls, with queued responses.
                _ => panic!("unknown test case"),
            }
            if operation != 2 {
                assert!(matches!(
                    pipeline.sync().await,
                    Err(crate::Error::ConnectionBroken)
                ));
                pipeline.cleanup().await; // Must not attempt a new drain.
            }
            drop(pipeline);
            assert!(conn.is_broken());
            assert!(matches!(
                conn.ping().await,
                Err(crate::Error::ConnectionBroken)
            ));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pipeline_consumed_errors_and_unclaimed_results_stay_usable() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        let mut pipeline = crate::tokio::pipeline::Pipeline::new_inner(&mut conn);
        let ticket = pipeline.exec("SELECT 1/$1", (0,)).unwrap();
        let error = [
            message(b'E', b"SERROR\0C22012\0Mdivision by zero\0\0"),
            message(b'Z', b"I"),
        ]
        .concat();
        let (result, ()) = tokio::join!(
            pipeline.claim_drop(ticket),
            respond(&mut peer, b'S', &error)
        );
        assert!(matches!(result, Err(crate::Error::Server(_))));
        let ticket = pipeline.exec("SELECT $1", (99,)).unwrap();
        let response = [
            message(b'1', &[]),
            message(b'2', &[]),
            rows(99, true),
            message(b'Z', b"I"),
        ]
        .concat();
        let (result, ()) = tokio::join!(
            pipeline.claim_collect::<(i32,)>(ticket),
            respond(&mut peer, b'S', &response)
        );
        assert_eq!(result.unwrap(), vec![(99,)]);
        pipeline.cleanup().await;
        drop(pipeline);
        assert!(!conn.is_broken());

        // Cleanup must also handle unclaimed prepared results without access to
        // the ticket's cached RowDescription.
        let stmt = crate::PreparedStatement {
            idx: 0,
            param_oids: vec![],
            row_desc_payload: None,
        };
        let mut pipeline = crate::tokio::pipeline::Pipeline::new_inner(&mut conn);
        let _ticket = pipeline.exec(&stmt, ()).unwrap();
        let response = [
            message(b'2', &[]),
            message(b'D', &[0, 1, 0, 0, 0, 4, 0, 0, 0, 42]),
            message(b'C', b"SELECT 1\0"),
            message(b'Z', b"I"),
        ]
        .concat();
        let ((), ()) = tokio::join!(pipeline.cleanup(), respond(&mut peer, b'S', &response));
        drop(pipeline);
        assert!(!conn.is_broken());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn callback_errors_decoding_errors_and_panics_poison_connection() {
    use std::future::Future;
    use std::task::Poll;
    timeout(LIMIT, async {
        for mode in 0..4 {
            let (mut conn, mut peer) = connection().await;
            let response = [rows(42, false), message(b'Z', b"I")].concat();
            let callback = |_: (i32,)| -> Result<()> {
                if mode == 1 {
                    panic!("intentional callback panic");
                }
                Err(crate::Error::InvalidUsage("callback stopped".into()))
            };
            let query = async {
                if mode == 2 {
                    conn.query_collect::<(bool,)>("SELECT 42").await.map(|_| ())
                } else if mode == 3 {
                    conn.exec_foreach("SELECT 42", (), callback).await
                } else {
                    conn.query_foreach("SELECT 42", callback).await
                }
            };
            let mut query = Box::pin(query);
            let catch = std::future::poll_fn(|cx| {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    query.as_mut().poll(cx)
                })) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
                    Err(panic) => Poll::Ready(Err(panic)),
                }
            });
            let response = if mode == 3 {
                [message(b'1', &[]), message(b'2', &[]), response].concat()
            } else {
                response
            };
            let (result, ()) = tokio::join!(
                catch,
                respond(&mut peer, if mode == 3 { b'S' } else { b'Q' }, &response)
            );
            if mode == 1 {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            drop(query);
            assert!(conn.is_broken());
            assert!(matches!(
                conn.ping().await,
                Err(crate::Error::ConnectionBroken)
            ));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn partial_write_cancellation_poison_connection() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        // Much larger than the socket send buffer; reading one byte cannot
        // complete write_all. Cancel while the frontend frame is incomplete.
        let sql = " ".repeat(4 * 1024 * 1024);
        tokio::select! {
        biased;
            _ = conn.query_drop(&sql) => panic!("oversized write completed without a reader"),
            first = peer.read_u8() => assert_eq!(first.unwrap(), b'Q'),
        }
        assert!(conn.is_broken());
        assert!(matches!(
            conn.ping().await,
            Err(crate::Error::ConnectionBroken)
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_during_partial_message_read() {
    timeout(LIMIT, async {
        for prefix in [vec![b'Z', 0], vec![b'Z', 0, 0, 0, 5]] {
            let (mut conn, mut peer) = connection().await;
            // Preload a CommandComplete followed by an incomplete header or
            // payload. The callback proves the driver reached that last frame.
            let (completed, received) = oneshot::channel();
            let mut handler = Completed(Some(completed));
            let server = async {
                respond(&mut peer, b'Q', &[message(b'C', b"DO\0"), prefix].concat()).await;
                received.await.unwrap();
            };
            tokio::select! {
                biased;
                _ = conn.query("DO $$ BEGIN END $$", &mut handler) => panic!("partial ReadyForQuery completed"),
                () = server => {}
            }
            assert!(conn.is_broken());
            assert!(matches!(conn.lowlevel_sync().await, Err(crate::Error::ConnectionBroken)));
        }
    }).await.unwrap();
}

#[tokio::test]
async fn lowlevel_completion_boundaries_allow_continuation() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        let bind = [message(b'1', &[]), message(b'2', &[])].concat();
        let (result, ()) = tokio::join!(
            conn.create_named_portal("p", &"SELECT 42", &()),
            respond(&mut peer, b'H', &bind)
        );
        result.unwrap();
        assert!(!conn.is_broken()); // BindComplete, no ReadyForQuery yet.
        let mut handler = crate::handler::DropHandler::new();
        let suspended = [message(b'n', &[]), message(b's', &[])].concat();
        let (result, ()) = tokio::join!(
            conn.lowlevel_execute("p", 1, &mut handler),
            respond(&mut peer, b'H', &suspended)
        );
        assert!(result.unwrap());
        assert!(!conn.is_broken()); // PortalSuspended, no ReadyForQuery yet.
        let complete = [message(b'n', &[]), message(b'C', b"SELECT 1\0")].concat();
        let (result, ()) = tokio::join!(
            conn.lowlevel_execute("p", 0, &mut handler),
            respond(&mut peer, b'H', &complete)
        );
        assert!(!result.unwrap());
        assert!(!conn.is_broken()); // CommandComplete, no ReadyForQuery yet.
        let closed = message(b'3', &[]);
        let (result, ()) = tokio::join!(
            conn.lowlevel_close_portal("p"),
            respond(&mut peer, b'H', &closed)
        );
        result.unwrap();
        assert!(!conn.is_broken());
        let ready = message(b'Z', b"I");
        let (result, ()) = tokio::join!(conn.lowlevel_sync(), respond(&mut peer, b'S', &ready));
        result.unwrap();
        assert!(!conn.is_broken());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn early_server_error_does_not_clear_unfinished_exchange() {
    timeout(LIMIT, async {
        for batch in [false, true] {
            let (mut conn, mut peer) = connection().await;
            let response = message(b'E', b"SERROR\0C42601\0Msyntax error\0\0");
            let operation = async {
                if batch {
                    conn.prepare_batch(&["invalid SQL"]).await.map(|_| ())
                } else {
                    conn.lowlevel_bind("p", "missing", ()).await
                }
            };
            let (result, ()) = tokio::join!(
                operation,
                respond(&mut peer, if batch { b'S' } else { b'H' }, &response)
            );
            assert!(matches!(result, Err(crate::Error::Server(_))));
            assert!(conn.is_broken());
            assert!(matches!(
                conn.ping().await,
                Err(crate::Error::ConnectionBroken)
            ));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pipeline_decode_error_cannot_be_hidden_by_cleanup() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        let mut pipeline = crate::tokio::pipeline::Pipeline::new_inner(&mut conn);
        let ticket = pipeline.exec("SELECT 42", ()).unwrap();
        let response = [
            message(b'1', &[]),
            message(b'2', &[]),
            rows(42, true),
            message(b'Z', b"I"),
        ]
        .concat();
        let (result, ()) = tokio::join!(
            pipeline.claim_collect::<(bool,)>(ticket),
            respond(&mut peer, b'S', &response)
        );
        assert!(matches!(result, Err(crate::Error::Decode(_))));
        pipeline.cleanup().await;
        assert!(matches!(
            pipeline.sync().await,
            Err(crate::Error::ConnectionBroken)
        ));
        drop(pipeline);
        assert!(conn.is_broken());
    })
    .await
    .unwrap();
}

#[tokio::test]
#[expect(
    clippy::mem_forget,
    reason = "exercise interruption without running Drop"
)]
async fn forgetting_an_inflight_future_does_not_restore_reuse() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        let mut query = Box::pin(conn.query_drop("SELECT 42"));
        tokio::select! {
            biased;
            _ = &mut query => panic!("query finished without a response"),
            () = respond(&mut peer, b'Q', &[]) => {}
        }
        // Rust allows forgetting a future and then using the borrowed Conn.
        // Unlike dropping it, this never runs destructors stored in the future.
        std::mem::forget(query);
        assert!(conn.is_broken());
        assert!(matches!(
            conn.query_drop("SELECT 99").await,
            Err(crate::Error::ConnectionBroken)
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fatal_pipeline_error_must_reject_further_operations() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        let mut pipeline = crate::tokio::pipeline::Pipeline::new_inner(&mut conn);
        let ticket = pipeline.exec("SELECT 42", ()).unwrap();
        pipeline.flush().await.unwrap();
        let fatal = message(b'E', b"SFATAL\0VFATAL\0C57P01\0Mterminating connection\0\0");
        let (result, ()) = tokio::join!(pipeline.claim_drop(ticket), async {
            respond(&mut peer, b'H', &fatal).await;
            peer.shutdown().await.unwrap();
        });
        let error = result.unwrap_err();
        assert!(error.is_connection_broken());
        assert!(
            matches!(
                pipeline.exec("SELECT 99", ()),
                Err(crate::Error::ConnectionBroken)
            ),
            "FATAL was consumed, but the pipeline accepts another operation"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[expect(
    clippy::mem_forget,
    reason = "exercise interruption without running Drop"
)]
async fn forgotten_pipeline_future_must_not_restore_reuse() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        let (sent, received) = oneshot::channel();
        let mut future = Box::pin(conn.pipeline(async |pipeline| {
            let ticket = pipeline.exec("SELECT 42", ())?;
            pipeline.flush().await?;
            pipeline.claim_drop(ticket).await?;
            pipeline.sync().await?;
            sent.send(()).unwrap();
            std::future::pending::<Result<()>>().await
        }));
        let server = async {
            let response = [message(b'1', &[]), message(b'2', &[]), rows(42, true)].concat();
            respond(&mut peer, b'H', &response).await;
            respond(&mut peer, b'S', &message(b'Z', b"I")).await;
            received.await.unwrap();
        };
        tokio::select! {
            biased;
            _ = &mut future => panic!("pipeline unexpectedly finished"),
            () = server => {}
        }
        std::mem::forget(future);
        assert!(conn.is_broken());
        let pool = Arc::new(Pool::new(Opts::default()));
        // No response to DISCARD ALL is supplied. Only the abandoned Sync's
        // ReadyForQuery is available, and must not let this connection enter the pool.
        pool.check_in(conn).await;
        assert!(
            pool.conns.is_empty(),
            "abandoned pipeline ReadyForQuery completed pool reset"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_portal_callback_must_not_restore_reuse() {
    timeout(LIMIT, async {
        let (mut conn, mut peer) = connection().await;
        let (executed, received) = oneshot::channel();
        let operation = conn.exec_portal("UPDATE t SET value = 42", (), async |portal| {
            let mut handler = crate::handler::DropHandler::new();
            portal.exec(0, &mut handler).await?;
            executed.send(()).unwrap();
            std::future::pending::<Result<()>>().await
        });
        let server = async {
            let bind = [message(b'1', &[]), message(b'2', &[])].concat();
            respond(&mut peer, b'H', &bind).await;
            let execute = [message(b'n', &[]), message(b'C', b"UPDATE 1\0")].concat();
            respond(&mut peer, b'H', &execute).await;
            received.await.unwrap();
        };
        tokio::select! {
            biased;
            _ = operation => panic!("portal unexpectedly finished"),
            () = server => {}
        }
        assert!(
            conn.is_broken(),
            "portal dropped before its final Sync, but Conn is reusable"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[expect(
    clippy::mem_forget,
    reason = "exercise interruption without running Drop"
)]
async fn fresh_pipeline_cannot_recover_abandoned_scope() {
    timeout(LIMIT, async {
        let (mut conn, _peer) = connection().await;
        let mut pipeline = crate::tokio::pipeline::Pipeline::new_inner(&mut conn);
        pipeline.sync().await.unwrap();
        std::mem::forget(pipeline);
        assert!(conn.is_broken());
        assert!(matches!(
            conn.lowlevel_sync().await,
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            conn.lowlevel_flush().await,
            Err(crate::Error::ConnectionBroken)
        ));
        let mut replacement = crate::tokio::pipeline::Pipeline::new_inner(&mut conn);
        assert!(matches!(
            replacement.exec("SELECT 99", ()),
            Err(crate::Error::ConnectionBroken)
        ));
        assert!(matches!(
            replacement.sync().await,
            Err(crate::Error::ConnectionBroken)
        ));
        replacement.cleanup().await;
        drop(replacement);
        assert!(conn.is_broken());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn completed_portal_scopes_preserve_reuse() {
    timeout(LIMIT, async {
        for callback_error in [false, true] {
            let (mut conn, mut peer) = connection().await;
            let operation = conn.exec_portal("SELECT 42", (), async |portal| {
                let mut handler = crate::handler::DropHandler::new();
                assert!(portal.exec(1, &mut handler).await?);
                assert!(!portal.exec(0, &mut handler).await?);
                if callback_error {
                    Err(crate::Error::InvalidUsage(
                        "callback stopped after fetch".into(),
                    ))
                } else {
                    Ok(())
                }
            });
            let server = async {
                let bind = [message(b'1', &[]), message(b'2', &[])].concat();
                respond(&mut peer, b'H', &bind).await;
                let suspended = [message(b'n', &[]), message(b's', &[])].concat();
                respond(&mut peer, b'H', &suspended).await;
                let complete = [message(b'n', &[]), message(b'C', b"SELECT 1\0")].concat();
                respond(&mut peer, b'H', &complete).await;
                respond(&mut peer, b'S', &message(b'Z', b"I")).await;
            };
            let (result, ()) = tokio::join!(operation, server);
            assert_eq!(result.is_err(), callback_error);
            assert!(!conn.is_broken());
            let response = [rows(99, false), message(b'Z', b"I")].concat();
            let (result, ()) = tokio::join!(
                conn.query_collect::<(i32,)>("SELECT 99"),
                respond(&mut peer, b'Q', &response)
            );
            assert_eq!(result.unwrap(), vec![(99,)]);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[expect(
    clippy::mem_forget,
    reason = "exercise interruption without running Drop"
)]
async fn forgotten_and_panicking_portal_callbacks_prevent_reuse() {
    use std::future::Future;
    use std::task::Poll;
    timeout(LIMIT, async {
        for panic in [false, true] {
            let (mut conn, mut peer) = connection().await;
            let (entered, received) = oneshot::channel();
            let mut operation = Box::pin(conn.exec_portal("SELECT 42", (), async |_| {
                entered.send(()).unwrap();
                if panic {
                    panic!("intentional portal callback panic");
                }
                std::future::pending::<Result<()>>().await
            }));
            let caught = std::future::poll_fn(|cx| {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    operation.as_mut().poll(cx)
                })) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(_)) => panic!("portal unexpectedly finished"),
                    Err(_) => Poll::Ready(()),
                }
            });
            let server = async {
                let bind = [message(b'1', &[]), message(b'2', &[])].concat();
                respond(&mut peer, b'H', &bind).await;
                received.await.unwrap();
            };
            tokio::select! {
                biased;
                () = caught => assert!(panic),
                () = server => assert!(!panic),
            }
            if panic {
                drop(operation);
            } else {
                std::mem::forget(operation);
            }
            assert!(conn.is_broken());
            assert!(matches!(
                conn.ping().await,
                Err(crate::Error::ConnectionBroken)
            ));
            assert!(matches!(
                conn.lowlevel_sync().await,
                Err(crate::Error::ConnectionBroken)
            ));
        }
    })
    .await
    .unwrap();
}
