
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

def generate_silence(duration_sec, output_path):
    subprocess.run([
        ffmpeg_exe, '-y', 
        '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo', 
        '-t', str(duration_sec),
        '-acodec', 'libmp3lame',
        output_path
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def process_speech_to_audio(message_body: bytes):
    video_id = None
    temp_files = [] # Keep track to remove later

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
            
        # Parse SRT to get segments: index -> {start_ms, end_ms, duration_ms}
        # Format:
        # 1
        # 00:00:00,000 --> 00:00:05,000
        # Text...
        
        srt_segments = []
        blocks = srt_content.strip().split('\n\n')
        for block in blocks:
            lines = block.strip().split('\n')
            if len(lines) >= 2:
                try:
                    index = int(lines[0].strip())
                    time_line = lines[1].strip()
                    start_str, end_str = time_line.split(' --> ')
                    
                    def parse_time(t_str):
                        h, m, s_ms = t_str.split(':')
                        s, ms = s_ms.split(',')
                        return int(h) * 3600000 + int(m) * 60000 + int(s) * 1000 + int(ms)

                    start_ms = parse_time(start_str.strip())
                    end_ms = parse_time(end_str.strip())
                    
                    srt_segments.append({
                        'index': index,
                        'start_ms': start_ms,
                        'end_ms': end_ms,
                        'duration_ms': end_ms - start_ms
                    })
                except Exception as e:
                    print(f"Error parsing srt block: {block[:50]}... - {e}")
                    continue
        
        if not srt_segments:
             print("No valid SRT segments found")
             return

        # 2. Get Voice Chunks
        chunks = list(voice_chunks_collection.find({'video_id': video_id}))
        # Map chunks by index for easy lookup
        chunks_map = {c.get('index'): c for c in chunks}
        
        # 3. Process Audio Generation
        # We need to build a timeline. 
        # Strategy:
        # - Create a silence.mp3 (1s) to usage
        # - Iterate through SRT segments.
        # - Calculate gap from previous end to current start -> append silence.
        # - Get audio for current segment -> processed audio -> append.
        
        # Create a silent audio file for gaps
        # ffmpeg -f lavfi -i anullsrc=r=44100:cl=stereo -t 1 -q:a 9 -acodec libmp3lame silence.mp3
        fd_silence, silence_base_path = tempfile.mkstemp(suffix=".mp3")
        os.close(fd_silence)
        temp_files.append(silence_base_path)
        # Use simple silence generator
        generate_silence(1, silence_base_path)

        concat_list_path = os.path.join(tempfile.gettempdir(), f"concat_{video_id}.txt")
        temp_files.append(concat_list_path)
        
        current_time_ms = 0
        final_parts = [] # List of tuples (file_path, duration_ms)
        
        with open(concat_list_path, 'w', encoding='utf-8') as f_concat:
            for seg in srt_segments:
                seg_index = seg['index']
                target_duration_ms = seg['duration_ms']
                
                # A. Handle Gap (Silence)
                gap_ms = seg['start_ms'] - current_time_ms
                if gap_ms > 0:
                     # Create silence of gap_ms duration
                     fd_gap, gap_path = tempfile.mkstemp(suffix=".mp3")
                     os.close(fd_gap)
                     temp_files.append(gap_path)
                     
                     gap_seconds = gap_ms / 1000.0
                     generate_silence(gap_seconds, gap_path)
                     
                     f_concat.write(f"file '{gap_path.replace(os.sep, '/')}'\n")

                # B. Handle Segment Audio
                chunk = chunks_map.get(seg_index)
                if chunk and chunk.get('request_id'):
                    request_id = chunk.get('request_id')
                    audio_url = None
                    
                    # Call Vbee API with Retry
                    for attempt in range(3):
                        try:
                            vbee_url = f"https://vbee.vn/api/v1/tts/{request_id}"
                            headers = {'Authorization': f"Bearer {settings.VBEE_TOKEN}"}
                            resp = requests.get(vbee_url, headers=headers)
                            resp_json = resp.json()
                            
                            # Check status
                            # "status": 1 means success? User provided sample response had status: 1
                            if resp_json.get('status') == 1 and resp_json.get('result', {}).get('audio_link'):
                                audio_url = resp_json['result']['audio_link']
                                break
                            elif resp_json.get('status') == 1 and resp_json.get('result', {}).get('status') == 'processing':
                                # Wait and retry
                                time.sleep(5)
                                continue
                            else:
                                print(f"Vbee error for {request_id}: {resp.text}")
                                time.sleep(5)
                        except Exception as e:
                            print(f"Exception calling Vbee {request_id}: {e}")
                            time.sleep(5)

                    if audio_url:
                        # Download Audio
                        fd_dl, dl_path = tempfile.mkstemp(suffix=".mp3")
                        os.close(fd_dl)
                        temp_files.append(dl_path)
                        
                        try:
                            with requests.get(audio_url, stream=True) as r:
                                r.raise_for_status()
                                with open(dl_path, 'wb') as f:
                                    for chunk_data in r.iter_content(chunk_size=8192):
                                        f.write(chunk_data)
                                        
                            # Get Duration using ffprobe (more reliable)
                            try:
                                # Run ffprobe to get duration in seconds
                                result = subprocess.run([
                                    'ffprobe', '-v', 'error', '-show_entries', 
                                    'format=duration', '-of', 
                                    'default=noprint_wrappers=1:nokey=1', 
                                    dl_path
                                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                                
                                original_duration_sec = float(result.stdout.strip())
                            except Exception as e:
                                print(f"Error checking duration for chunk {seg_index}: {e}")
                                # Use ffmpeg fallback if ffprobe fails or not found
                                result = subprocess.run([ffmpeg_exe, '-i', dl_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                                import re
                                duration_match = re.search(r"Duration: (\d{2}):(\d{2}):(\d{2}\.\d+)", result.stderr)
                                if duration_match:
                                    h, m, s = duration_match.groups()
                                    original_duration_sec = int(h)*3600 + int(m)*60 + float(s)
                                else:
                                    print(f"Could not determine duration for chunk {seg_index}, skipping speed adjustment.")
                                    original_duration_sec = target_duration_ms / 1000.0

                            target_duration_sec = target_duration_ms / 1000.0
                            
                            # Speed adjustment logic:
                            # - Speech LONGER than target → speed up to fit
                            # - Speech SHORTER than target → keep original, pad silence
                            
                            if original_duration_sec > target_duration_sec:
                                # Speech dài hơn target → Tăng tốc để khớp
                                tempo = original_duration_sec / target_duration_sec
                                tempo = max(0.5, min(2.0, tempo))
                                
                                fd_fixed, fixed_path = tempfile.mkstemp(suffix=".mp3")
                                os.close(fd_fixed)
                                temp_files.append(fixed_path)

                                subprocess.run([
                                    ffmpeg_exe, '-y',
                                    '-i', dl_path,
                                    '-filter:a', f"atempo={tempo}",
                                    '-vn',
                                    fixed_path
                                ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                                
                                f_concat.write(f"file '{fixed_path.replace(os.sep, '/')}'\n")
                            else:
                                # Speech ngắn hơn hoặc bằng target → Giữ nguyên tốc độ
                                f_concat.write(f"file '{dl_path.replace(os.sep, '/')}'\n")
                                
                                # Chèn khoảng lặng để lấp đầy thời gian còn lại
                                padding_sec = target_duration_sec - original_duration_sec
                                if padding_sec > 0.05:  # Chỉ thêm nếu khoảng lặng > 50ms
                                    fd_pad, pad_path = tempfile.mkstemp(suffix=".mp3")
                                    os.close(fd_pad)
                                    temp_files.append(pad_path)
                                    generate_silence(padding_sec, pad_path)
                                    f_concat.write(f"file '{pad_path.replace(os.sep, '/')}'\n")

                        except Exception as e:
                            print(f"Error processing audio chunk {seg_index}: {e}")
                            # Clean up invalid file from list if possible, or just continue
                            continue
                else: 
                     # No chunk? Add silence matching duration to keep sync
                     fd_silence_chunk, silence_chunk_path = tempfile.mkstemp(suffix=".mp3")
                     os.close(fd_silence_chunk)
                     temp_files.append(silence_chunk_path)
                     generate_silence(target_duration_ms / 1000.0, silence_chunk_path)
                     f_concat.write(f"file '{silence_chunk_path.replace(os.sep, '/')}'\n")

                current_time_ms = seg['end_ms']

        # 4. Concatenate All
        final_output_path = os.path.join(tempfile.gettempdir(), f"final_{video_id}.mp3")
        temp_files.append(final_output_path)
        
        try:
           subprocess.run([
               ffmpeg_exe, '-y',
               '-f', 'concat',
               '-safe', '0',
               '-i', concat_list_path,
               '-c', 'copy',
               final_output_path
           ], check=True) #, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
           
           # 5. Upload to MinIO
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
           
           # 6. Update Video Record
           video_collection.update_one(
                {'_id': ObjectId(video_id)},
                {'$set': {'sub.final_audio_key': final_s3_key}}
           )
           print(f"Speech processing complete for {video_id}. Key: {final_s3_key}")

           # 7. Trigger Edit Process
           from app.services.rabbit_service import publish_event
           publish_event('topic_edit_process', {'videoId': video_id})
           print(f"Triggered edit process for {video_id}")

        except subprocess.CalledProcessError as e:
            print(f"Error concatenating audio: {e}")
        
    except Exception as e:
        print(f"Fatal error in process_speech_to_audio: {e}")

    finally:
        # Cleanup
        for p in temp_files:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except:
                    pass
