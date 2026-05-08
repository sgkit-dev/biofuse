"""Plink-server subprocess.

A standalone server that owns one ``VczReader`` and serves PLINK 1.9
view requests over an ``AF_UNIX`` listening socket. Each accepted
connection runs in its own daemon thread with its own ``BedEncoder``,
so different consumers of the same ``.bed`` get independent streaming
state and run concurrently.

Wire protocol is :mod:`biofuse.plink_protocol` — synchronous,
per-socket, no seq ids: the socket *is* the channel. Server threads
serve one request at a time on their socket and exit cleanly when the
parent half-closes.
"""

import logging
import select
import socket
import threading
import time

from vcztools import cli as vcztools_cli
from vcztools import plink as vcztools_plink
from vcztools import retrieval as vcztools_retrieval

from biofuse import plink_protocol

logger = logging.getLogger(__name__)


class _ServerSession:
    """Server-side state shared across all connection threads.

    Holds the ``VczReader``, the precomputed ``.bim`` and ``.fam``
    bytes, and the ``.bed`` size. Immutable after construction; safe
    to read concurrently from any thread without locking.
    """

    def __init__(self, reader: vcztools_retrieval.VczReader) -> None:
        self.reader = reader
        bim_text = vcztools_plink.generate_bim(reader)
        fam_text = vcztools_plink.generate_fam(reader)
        self.bim_bytes = bim_text.encode("utf-8")
        self.fam_bytes = fam_text.encode("utf-8")
        # ``reader.num_variants`` is the raw store count; with a
        # variant-chunk plan in effect (set_variants / materialised
        # filter) the BedEncoder emits one row per *planned* variant.
        num_planned_variants = int(reader.variant_counts_per_chunk().sum())
        num_samples = int(reader.sample_ids.size)
        bytes_per_variant = (num_samples + 3) // 4
        self.bed_size = 3 + num_planned_variants * bytes_per_variant


def _recv_exact_sync(sock: socket.socket, n: int) -> bytes:
    """Read exactly ``n`` bytes off ``sock``. Returns ``b""`` on clean EOF.

    Raises :class:`EOFError` if EOF arrives mid-frame.
    """
    if n == 0:
        return b""
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        chunk = sock.recv_into(view[got:], n - got)
        if chunk == 0:
            if got == 0:
                return b""
            raise EOFError(f"socket closed mid-frame after {got}/{n} bytes")
        got += chunk
    return bytes(buf)


def _handle_connection(conn_sock: socket.socket, session: _ServerSession) -> None:
    """Run one client connection synchronously. Returns on EOF.

    A ``BedEncoder`` is constructed up front for every connection and
    its lifetime is bound to the connection via a ``with`` block. If
    construction fails, an errno reply is written to the socket so the
    client surfaces a real ``OSError`` rather than an unexplained EOF.
    """
    tname = threading.current_thread().name
    logger.debug("%s: conn accepted", tname)
    with conn_sock:
        try:
            t_enc = time.monotonic()
            encoder_cm = vcztools_plink.BedEncoder(session.reader)
        except Exception as exc:  # noqa: BLE001 - any error becomes errno reply
            err = plink_protocol.errno_for_exception(exc)
            if not isinstance(exc, OSError):
                logger.exception(
                    "plink-server BedEncoder construction failed; replying with errno"
                )
            try:
                conn_sock.sendall(plink_protocol.pack_error_reply(err))
            except OSError as send_exc:
                logger.warning("plink-server send failed: %s", send_exc)
            return
        with encoder_cm as encoder:
            logger.debug(
                "%s: encoder created in %.3fs", tname, time.monotonic() - t_enc
            )
            while True:
                try:
                    tag_buf = _recv_exact_sync(conn_sock, 1)
                except EOFError as exc:
                    logger.warning("plink-server frame error: %s", exc)
                    return
                if len(tag_buf) == 0:
                    return
                tag = bytes(tag_buf)
                try:
                    if tag == plink_protocol.TAG_GET_METADATA:
                        reply = plink_protocol.pack_metadata_reply(
                            session.bim_bytes, session.fam_bytes, session.bed_size
                        )
                    elif tag == plink_protocol.TAG_READ:
                        payload = _recv_exact_sync(
                            conn_sock, plink_protocol.REQ_READ_PAYLOAD_SIZE
                        )
                        if len(payload) < plink_protocol.REQ_READ_PAYLOAD_SIZE:
                            return
                        off, size = plink_protocol.parse_read_payload(payload)
                        t_read = time.monotonic()
                        data = encoder.read(off, size)
                        logger.debug(
                            "%s: encoder.read off=%d size=%d in %.3fs",
                            tname,
                            off,
                            size,
                            time.monotonic() - t_read,
                        )
                        reply = plink_protocol.pack_read_reply(data)
                    else:
                        logger.warning(
                            "plink-server: unknown tag %r; closing connection", tag
                        )
                        return
                except Exception as exc:  # noqa: BLE001 - any error becomes errno reply
                    err = plink_protocol.errno_for_exception(exc)
                    if not isinstance(exc, OSError):
                        logger.exception(
                            "plink-server dispatch raised; replying with EIO"
                        )
                    reply = plink_protocol.pack_error_reply(err)
                try:
                    conn_sock.sendall(reply)
                except OSError as exc:
                    logger.warning("plink-server send failed: %s", exc)
                    return
    logger.debug("%s: conn thread exit", tname)


def serve_forever(
    listener_sock: socket.socket,
    stop_sock: socket.socket,
    session: _ServerSession,
) -> None:
    """Accept loop. Spawns one daemon thread per connection.

    Returns when ``stop_sock`` becomes readable (the parent closed
    its end of the socketpair). Closes ``listener_sock`` on the way
    out. Connection threads are daemon threads; they exit on their
    own once their client closes the socket. We do not join them on
    shutdown — the process exit reaps them.
    """
    try:
        while True:
            try:
                readable, _, _ = select.select([listener_sock, stop_sock], [], [])
            except OSError as exc:
                logger.warning("plink-server select failed: %s", exc)
                return
            if stop_sock in readable:
                return
            try:
                conn_sock, _ = listener_sock.accept()
            except OSError as exc:
                logger.warning("plink-server accept failed: %s", exc)
                return
            t = threading.Thread(
                target=_handle_connection,
                args=(conn_sock, session),
                name="plink-conn",
                daemon=True,
            )
            try:
                t.start()
            except RuntimeError as exc:
                # Per-process thread budget exhausted (e.g. cgroup pids
                # limit). Decline this connection cleanly so the server
                # stays alive; the client sees an EOF on the half-open
                # socket and surfaces it as ``OSError`` to the FUSE
                # layer.
                logger.warning(
                    "plink-server: thread.start() failed (%s); active=%d",
                    exc,
                    threading.active_count(),
                )
                try:
                    conn_sock.close()
                except OSError:
                    pass
                continue
    finally:
        try:
            listener_sock.close()
        except OSError:
            pass


def _server_main(
    listener_sock: socket.socket,
    stop_sock: socket.socket,
    vcz_url: str,
    reader_options: vcztools_cli.ViewPlinkOptions,
    log_config: vcztools_cli.LogConfig,
) -> None:
    """Subprocess entry point invoked via ``multiprocessing.Process``.

    The two sockets are handle-passed by ``multiprocessing`` (the
    reduction machinery dups the fds into the child). The reader is
    used as a context manager so its shared ``ThreadPoolExecutor``
    (one pool per reader, drawn on by every ``BedEncoder`` /
    ``ReadaheadPipeline``) is drained on the way out.

    ``log_config`` matches the parent's verbosity so the subprocess's
    own ``logger.debug`` / ``logger.info`` output reaches the same sink
    as the parent. ``reader_options`` carries the bcftools-style
    filtering options (regions, samples, …) that vcztools'
    ``make_reader`` consumes.

    Any exception raised before ``serve_forever`` starts (reader
    construction, ``_ServerSession`` construction — the latter eagerly
    walks the variants to build ``.bim`` / ``.fam`` and rejects e.g.
    multi-allelic input) is caught here so multiprocessing's default
    handler does not print a traceback. The cause is logged at ERROR
    (visible at default verbosity); the traceback only surfaces at
    DEBUG.
    """
    log_config.apply()
    try:
        with vcztools_cli.make_reader_from_options(vcz_url, reader_options) as reader:
            # Bcftools-style filters (--max-alleles, --types, --include,
            # …) configure a per-variant predicate via
            # ``reader.set_variant_filter``; vcztools' ``BedEncoder``
            # refuses readers in that state. Resolve the predicate now
            # into a fixed surviving-variant chunk plan so each
            # connection's encoder sees a plain reader. No-op when no
            # variant filter is configured.
            reader.materialise_variant_filter()
            session = _ServerSession(reader)
            try:
                serve_forever(listener_sock, stop_sock, session)
            finally:
                try:
                    stop_sock.close()
                except OSError:
                    pass
    except Exception as exc:  # noqa: BLE001 - cleanly surface any startup failure
        logger.error("plink-server startup failed: %s", exc)
        logger.debug("plink-server startup traceback", exc_info=True)
        try:
            listener_sock.close()
        except OSError:
            pass
        try:
            stop_sock.close()
        except OSError:
            pass
