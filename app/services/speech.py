
import os
import json
import time
import requests
import tempfile
import subprocess
from bson import ObjectId
from app.core.database import video_collection, voice_chunks_collection, minio_client
from app.core.config import settings

import imageio_ffmpeg

ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()


def generate_silence(duration_sec: float, output_path: str):
    """Generate a silent MP3 of exact duration (millisecond-precise)."""
    subprocess.run([
        ffmpeg_exe, '-y',
        '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
        '-t', f"{duration_sec:.6f}",   # 6 decimal places = microsecond precision
        '-acodec', 'libmp3lame',
        '-ar', '44100',
        '-ac', '2',
        '-b:a', '128k',
        output_path
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def build_atempo_chain(tempo: float) -> str:
    """
    Build an FFmpeg audio filter chain to achieve any tempo ratio.

    atempo only supports [0.5, 2.0]. For values outside this range,
    we chain multiple atempo filters.

    Examples:
      tempo=3.0  → "atempo=2.0,atempo=1.5"
      tempo=0.25 → "atempo=0.5,atempo=0.5"
      tempo=1.3  → "atempo=1.3"
    """
    filters = []

    # Handle tempo > 2.0 by chaining atempo=2.0 repeatedly
    while tempo > 2.0:
        filters.append("atempo=2.0")
        tempo /= 2.0

    # Handle tempo < 0.5 by chaining atempo=0.5 repeatedly
    while tempo < 0.5:
        filters.append("atempo=0.5")
        tempo *= 2.0

    # Append the final remainder (always in [0.5, 2.0])
    filters.append(f"atempo={tempo:.6f}")

    return ",".join(filters)


def get_audio_duration_sec(path: str) -> float:
    """Use ffprobe to get audio file duration in seconds. Raises on failure."""
    result = subprocess.run([
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        path
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"ffprobe failed: {result.stderr.strip()}")

    return float(result.stdout.strip())


def speed_adjust_audio(src_path: str, target_duration_sec: float, temp_files: list) -> str:
    """
    Adjust audio speed so it exactly matches target_duration_sec.

    - If longer: speed up using chained atempo (no clamp), then trim to exact length.
    - If shorter: keep original speed and append silence padding.
    - Returns path to the final adjusted audio file.
    """
    original_duration_sec = get_audio_duration_sec(src_path)
    delta_sec = original_duration_sec - target_duration_sec

    print(f"  Audio sync: original={original_duration_sec:.3f}s, "
          f"target={target_duration_sec:.3f}s, delta={delta_sec:.3f}s")

    # Case 1: Already matches (within 20ms tolerance) — use as-is
    if abs(delta_sec) <= 0.02:
        return src_path

    # Case 2: TTS audio is longer than target → speed up
    if original_duration_sec > target_duration_sec:
        tempo = original_duration_sec / target_duration_sec
        atempo_chain = build_atempo_chain(tempo)

        fd, sped_path = tempfile.mkstemp(suffix=".mp3")
        os.close(fd)
        temp_files.append(sped_path)

        subprocess.run([
            ffmpeg_exe, '-y',
            '-i', src_path,
            '-filter:a', atempo_chain,
            '-ar', '44100', '-ac', '2', '-b:a', '128k',
            sped_path
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Hard trim to exact target length to absorb any rounding error
        fd2, trimmed_path = tempfile.mkstemp(suffix=".mp3")
        os.close(fd2)
        temp_files.append(trimmed_path)

        subprocess.run([
            ffmpeg_exe, '-y',
            '-i', sped_path,
            '-t', f"{target_duration_sec:.6f}",
            '-c', 'copy',
            trimmed_path
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        return trimmed_path

    # Case 3: TTS audio is shorter than target → pad with silence
    padding_sec = target_duration_sec - original_duration_sec

    fd_pad, pad_path = tempfile.mkstemp(suffix=".mp3")
    os.close(fd_pad)
    temp_files.append(pad_path)
    generate_silence(padding_sec, pad_path)

    # Concat original + silence into one file
    fd_list, list_path = tempfile.mkstemp(suffix=".txt")
    os.close(fd_list)
    temp_files.append(list_path)

    with open(list_path, 'w') as f:
        f.write(f"file '{src_path.replace(os.sep, '/')}'\n")
        f.write(f"file '{pad_path.replace(os.sep, '/')}'\n")

    fd_out, out_path = tempfile.mkstemp(suffix=".mp3")
    os.close(fd_out)
    temp_files.append(out_path)

    subprocess.run([
        ffmpeg_exe, '-y',
        '-f', 'concat', '-safe', '0',
        '-i', list_path,
        '-c', 'copy',
        out_path
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Hard trim to exact target length (absorb MP3 frame rounding)
    fd_trim, trim_path = tempfile.mkstemp(suffix=".mp3")
    os.close(fd_trim)
    temp_files.append(trim_path)

    subprocess.run([
        ffmpeg_exe, '-y',
        '-i', out_path,
        '-t', f"{target_duration_sec:.6f}",
        '-c', 'copy',
        trim_path
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    return trim_path


def parse_srt(srt_content: str) -> list:
    """Parse SRT content into list of {index, start_ms, end_ms, duration_ms}."""
    segments = []
    blocks = srt_content.strip().split('\n\n')

    for block in blocks:
        lines = block.strip().split('\n')
        if len(lines) < 2:
            continue
        try:
            index = int(lines[0].strip())
            time_line = lines[1].strip()
            start_str, end_str = time_line.split(' --> ')

            def parse_time(t: str) -> int:
                h, m, s_ms = t.strip().split(':')
                s, ms = s_ms.split(',')
                return int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)

            start_ms = parse_time(start_str)
            end_ms = parse_time(end_str)
            segments.append({
                'index': index,
                'start_ms': start_ms,
                'end_ms': end_ms,
                'duration_ms': end_ms - start_ms
            })
        except Exception as e:
            print(f"  Warning: Failed to parse SRT block: {block[:60]!r} — {e}")
            continue

    return segments


def process_speech_to_audio(message_body: bytes):
    video_id = None
    temp_files = []

    try:
        data = json.loads(message_body)
        video_id = data.get('video_id')
        print(f"Processing Speech to Audio for Video: {video_id}")

        if not video_id:
            print("No video_id in message")
            return

        # 1. Get Video Data & SRT
        video = video_collection.find_one({'_id': ObjectId(video_id)})
        if not video:
            print(f"Video {video_id} not found")
            return

        srt_content = video.get('sub', {}).get('srt_concatenated', '')
        if not srt_content:
            print(f"No SRT content for video {video_id}")
            return

        # 2. Parse SRT
        srt_segments = parse_srt(srt_content)
        if not srt_segments:
            print("No valid SRT segments found")
            return

        print(f"Parsed {len(srt_segments)} SRT segments")

        # 3. Get Voice Chunks map
        chunks = list(voice_chunks_collection.find({'video_id': video_id}))
        chunks_map = {c.get('index'): c for c in chunks}

        # 4. Build timeline — concat list file
        concat_list_path = os.path.join(tempfile.gettempdir(), f"concat_{video_id}.txt")
        temp_files.append(concat_list_path)

        current_time_ms = 0  # Tracks our position in the output timeline

        with open(concat_list_path, 'w', encoding='utf-8') as f_concat:
            for seg in srt_segments:
                seg_index = seg['index']
                target_duration_ms = seg['duration_ms']
                target_duration_sec = target_duration_ms / 1000.0

                # --- A. Gap Silence (before this segment starts) ---
                gap_ms = seg['start_ms'] - current_time_ms
                if gap_ms > 0:
                    gap_sec = gap_ms / 1000.0
                    fd_gap, gap_path = tempfile.mkstemp(suffix=".mp3")
                    os.close(fd_gap)
                    temp_files.append(gap_path)
                    generate_silence(gap_sec, gap_path)
                    f_concat.write(f"file '{gap_path.replace(os.sep, '/')}'\n")
                    print(f"  Seg {seg_index}: gap silence {gap_ms}ms")
                elif gap_ms < 0:
                    # Overlapping segments — skip forward
                    print(f"  WARNING: Seg {seg_index} starts {-gap_ms}ms before current time. Skipping gap.")

                # --- B. Segment Audio ---
                chunk = chunks_map.get(seg_index)
                segment_written = False

                if chunk and chunk.get('request_id'):
                    request_id = chunk.get('request_id')
                    audio_url = None

                    # Fetch TTS audio URL from Vbee with retry
                    for attempt in range(3):
                        try:
                            vbee_url = f"https://vbee.vn/api/v1/tts/{request_id}"
                            headers = {'Authorization': f"Bearer {settings.VBEE_TOKEN}"}
                            resp = requests.get(vbee_url, headers=headers, timeout=30)
                            resp_json = resp.json()

                            if resp_json.get('status') == 1 and resp_json.get('result', {}).get('audio_link'):
                                audio_url = resp_json['result']['audio_link']
                                break
                            elif resp_json.get('status') == 1 and resp_json.get('result', {}).get('status') == 'processing':
                                time.sleep(5)
                                continue
                            else:
                                print(f"  Vbee error (attempt {attempt+1}) for {request_id}: {resp.text[:200]}")
                                time.sleep(5)
                        except Exception as e:
                            print(f"  Exception calling Vbee {request_id} (attempt {attempt+1}): {e}")
                            time.sleep(5)

                    if audio_url:
                        fd_dl, dl_path = tempfile.mkstemp(suffix=".mp3")
                        os.close(fd_dl)
                        temp_files.append(dl_path)

                        try:
                            with requests.get(audio_url, stream=True, timeout=60) as r:
                                r.raise_for_status()
                                with open(dl_path, 'wb') as f:
                                    for chunk_data in r.iter_content(chunk_size=8192):
                                        f.write(chunk_data)

                            print(f"  Seg {seg_index}: adjusting audio to {target_duration_sec:.3f}s")
                            adjusted_path = speed_adjust_audio(dl_path, target_duration_sec, temp_files)
                            f_concat.write(f"file '{adjusted_path.replace(os.sep, '/')}'\n")
                            segment_written = True

                        except Exception as e:
                            print(f"  Error processing audio for seg {seg_index}: {e}")
                            # Fall through to silence fallback

                # Fallback: silence for the full segment duration (no chunk or download failed)
                if not segment_written:
                    print(f"  Seg {seg_index}: no audio, using silence ({target_duration_ms}ms)")
                    fd_sil, sil_path = tempfile.mkstemp(suffix=".mp3")
                    os.close(fd_sil)
                    temp_files.append(sil_path)
                    generate_silence(target_duration_sec, sil_path)
                    f_concat.write(f"file '{sil_path.replace(os.sep, '/')}'\n")

                # Always advance timeline to end of this segment
                current_time_ms = seg['end_ms']

        # 5. Concatenate all parts
        final_output_path = os.path.join(tempfile.gettempdir(), f"final_{video_id}.mp3")
        temp_files.append(final_output_path)

        print(f"Concatenating audio timeline for {video_id}...")
        subprocess.run([
            ffmpeg_exe, '-y',
            '-f', 'concat', '-safe', '0',
            '-i', concat_list_path,
            '-ar', '44100', '-ac', '2', '-b:a', '128k',
            final_output_path
        ], check=True)

        # 6. Upload to MinIO
        final_s3_key = f"audios/{video_id}/final_tts.mp3"
        with open(final_output_path, 'rb') as f_final:
            file_stat = os.stat(final_output_path)
            minio_client.put_object(
                settings.MINIO_BUCKET,
                final_s3_key,
                f_final,
                file_stat.st_size,
                content_type="audio/mpeg"
            )

        # 7. Update Video Record
        video_collection.update_one(
            {'_id': ObjectId(video_id)},
            {'$set': {'sub.final_audio_key': final_s3_key}}
        )
        print(f"Speech processing complete for {video_id}. Key: {final_s3_key}")

        # 8. Trigger Edit Process
        from app.services.rabbit_service import publish_event
        publish_event('topic_edit_process', {'videoId': video_id})
        print(f"Triggered edit process for {video_id}")

    except Exception as e:
        print(f"Fatal error in process_speech_to_audio: {e}")

    finally:
        for p in temp_files:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass
