use rusteron_archive::*;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

const CHANNEL: &str = "aeron:ipc";
const STREAM_ID: i32 = 16;
const REPLAY_STREAM_ID: i32 = 17;
const MESSAGE_COUNT: i64 = 1_000_000;
const REPLAY_IDLE_TIMEOUT: Duration = Duration::from_secs(5);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(" Aeron Archive Replay Test - External Archive \n");

    println!("Connecting to external archive...");
    println!("Expected directories:");
    println!("  - Aeron: /tmp/rusteron_aeron");
    println!("  - Archive: /tmp/rusteron_archive \n");

    let aeron_ctx = AeronContext::new()?;
    aeron_ctx.set_dir(&cformat!("/tmp/rusteron_aeron"))?;
    aeron_ctx.set_client_name(&cformat!("replay_reproduction_client"))?;

    let aeron = Aeron::new(&aeron_ctx)?;
    aeron.start()?;

    sleep(Duration::from_millis(100));

    let archive_ctx = AeronArchiveContext::new()?;
    archive_ctx.set_aeron(&aeron)?;
    archive_ctx.set_control_request_channel(&cformat!("aeron:udp?endpoint=localhost:8010"))?;
    archive_ctx.set_control_response_channel(&cformat!("aeron:udp?endpoint=localhost:8020"))?;
    archive_ctx.set_recording_events_channel(&cformat!(
        "aeron:udp?control-mode=dynamic|control=localhost:8030"
    ))?;
    // set_no_credentials_supplier / set_idle_strategy were removed as separate helpers in 0.2.x —
    // just don't call them; the archive context defaults to no credentials / default idle strategy.

    let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_ctx, &aeron)?
        .poll_blocking(Duration::from_secs(10))?;
    println!("Connected to archive ID: {}\n", archive.get_archive_id());

    // Start recording
    let channel_cstr = cformat!("{CHANNEL}");
    println!("Starting recording on {}:{}", CHANNEL, STREAM_ID);

    let recording_subscription_id =
        archive.start_recording(&channel_cstr, STREAM_ID, SOURCE_LOCATION_LOCAL, true)?;

    println!("Recording started (subscription ID: {})\n", recording_subscription_id);

    // Create publication
    println!("Publishing {} messages...", MESSAGE_COUNT);
    let publication = aeron.add_exclusive_publication(&channel_cstr, STREAM_ID, Duration::from_secs(5))?;

    while !publication.is_connected() {
        print!(".");
        std::io::Write::flush(&mut std::io::stdout())?;
        sleep(Duration::from_millis(100));
    }

    // Publish messages
    let start = Instant::now();
    let mut published = 0i64;

    for i in 0..MESSAGE_COUNT {
        let bytes = i.to_le_bytes();
        loop {
            match publication.offer(&bytes) {
                Ok(_) => {
                    published += 1;
                    if published % 100_000 == 0 {
                        print!(".");
                        std::io::Write::flush(&mut std::io::stdout())?;
                    }
                    break;
                }
                Err(e) if e.is_retryable() => {
                    sleep(Duration::from_micros(1));
                }
                Err(e) => {
                    println!("breaking due to some error: {e}");
                    break;
                }
            }
        }
    }

    println!("Published {} messages in {:?}\n", published, start.elapsed());

    // Finalize recording
    drop(publication);
    sleep(Duration::from_millis(100));

    // Find the recording
    println!("Finding recording...");
    let mut recording_id = -1i64;
    let mut recording_start = 0i64;
    let mut recording_stop = 0i64;
    let mut count = 0i32;

    archive.list_recordings_for_uri_fn(
        &mut count,
        0,
        100,
        &channel_cstr,
        STREAM_ID,
        |desc: AeronArchiveRecordingDescriptor| {
            let id = desc.recording_id;
            let start = desc.start_position;
            let stop = desc.stop_position;

            println!("Recording {}: {} bytes", id, stop - start);

            if id > recording_id {
                recording_id = id;
                recording_start = start;
                recording_stop = stop;
            }
        },
    )?;

    if recording_id < 0 {
        return Err("No recording found!".into());
    }

    let recording_length = recording_stop - recording_start;
    let bytes_per_message = 64; // 32-byte header + 8 payload + 24 padding
    let expected_bytes = bytes_per_message * MESSAGE_COUNT;

    println!("\nRecording details:");
    println!("ID: {}", recording_id);
    println!("Size: {} bytes", recording_length);
    println!("Expected: {} bytes", expected_bytes);
    println!("Bytes per message: {}", recording_length / MESSAGE_COUNT);

    if recording_length != expected_bytes {
        println!("WARNING: Size mismatch!");
    }

    // Start replay FIRST, then subscribe scoped to the session id
    println!("\nStarting replay...");
    let replay_params = AeronArchiveReplayParams::builder()
        .position(recording_start)
        .length(recording_length)
        .file_io_max_length(64 * 1024 * 1024) // 64MB file I/O buffer
        .build()?;

    let replay_session_id =
        archive.start_replay(recording_id, &channel_cstr, REPLAY_STREAM_ID, &replay_params)?;

    println!("Replay session started: {}", replay_session_id);

    // Surface any archive-side error/abort reason immediately
    archive.poll_for_recording_signals()?;
    let err = archive.poll_for_error_response_as_string(4096)?;
    if !err.is_empty() {
        println!("Archive error after start_replay: {}", err);
    }

    // Session-id-scoped subscription so we only pick up this replay's image
    let replay_channel = ChannelUri::add_session_id(CHANNEL, replay_session_id as i32).into_c_string();

    println!(
        "Setting up replay subscription on stream {} (session {})",
        REPLAY_STREAM_ID, replay_session_id as i32 as u32
    );
    let replay_subscription = aeron.add_subscription(
        &replay_channel,
        REPLAY_STREAM_ID,
        Handlers::NONE,
        Handlers::NONE,
        Duration::from_secs(5),
    )?;

    sleep(Duration::from_millis(100));

    // Poll for messages
    println!("\nPolling for replayed messages...");
    let received_count = AtomicUsize::new(0);
    let last_value = AtomicI64::new(-1);
    let first_value = AtomicI64::new(-1);

    let start_replay = Instant::now();
    let mut last_progress_time = Instant::now();

    while received_count.load(Ordering::Relaxed) < MESSAGE_COUNT as usize {
        let fragments = replay_subscription.poll_fn(
            |buffer: &[u8], _header: AeronHeader| {
                if buffer.len() >= 8 {
                    let value = i64::from_le_bytes([
                        buffer[0], buffer[1], buffer[2], buffer[3],
                        buffer[4], buffer[5], buffer[6], buffer[7],
                    ]);

                    let count = received_count.fetch_add(1, Ordering::Relaxed);

                    if count == 0 {
                        first_value.store(value, Ordering::Relaxed);
                    }
                    last_value.store(value, Ordering::Relaxed);

                    if (count + 1) % 100_000 == 0 {
                        print!(".");
                        std::io::Write::flush(&mut std::io::stdout()).ok();
                    }
                }
            },
            100_000,
        )?;

        if fragments == 0 {
            if last_progress_time.elapsed() > REPLAY_IDLE_TIMEOUT {
                println!(
                    "\n\nReplay stalled — no data for {:?} (received {} so far)",
                    REPLAY_IDLE_TIMEOUT,
                    received_count.load(Ordering::Relaxed)
                );
                break;
            }
            std::thread::yield_now();
        } else {
            last_progress_time = Instant::now();
        }
    }

   
    if received_count.load(Ordering::Relaxed) < MESSAGE_COUNT as usize {
        if let Err(e) = archive.stop_replay(replay_session_id) {
            println!("stop_replay after early exit failed (likely already closed): {e}");
        }
    }

    let final_count = received_count.load(Ordering::Relaxed);
    let first = first_value.load(Ordering::Relaxed);
    let last = last_value.load(Ordering::Relaxed);

    println!("Published: {} messages", published);
    println!("Replayed:  {} messages", final_count);
    println!("First value: {} (expected 0)", first);
    println!("Last value:  {} (expected {})", last, MESSAGE_COUNT - 1);
    println!("Publish time: {:?}", start.elapsed());
    println!("Replay time:  {:?}", start_replay.elapsed());

    let percentage = (final_count as f64 / published as f64) * 100.0;

    if final_count as i64 == published && first == 0 && last == MESSAGE_COUNT - 1 {
        println!("\n All messages replayed correctly!");
        Ok(())
    } else {
        println!(
            "\nExpected {} messages but replayed {} ({:.2}%)",
            published, final_count, percentage
        );
        Err(format!("Unexpected replay count: {final_count}/{published}").into())
    }
}