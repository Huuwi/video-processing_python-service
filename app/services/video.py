
import json
import requests
import io
import os
import tempfile
import math
import subprocess # Add subprocess for calling ffmpeg
from bson import ObjectId # Import ObjectId for MongoDB queries
import imageio_ffmpeg
from app.core.database import video_collection, minio_client, audio_collection, user_collection
from app.core.config import settings
from app.services.rabbit_service import publish_event
from pathlib import Path
import ffmpeg as ffmpeg_lib # Alias to avoid conflicts
import sys


def fetch_video_info(url: str):
    """
    Attempts to fetch video info using Hybrid API first, then falls back to specific platform APIs.
    Returns a dictionary with 'duration', 'download_link', 'desc', 'cover', etc.
    """
    
    # 1. Try Hybrid API (Simpler logic as requested by user + Bilibili support)
    try:
        print(f"Attempting Hybrid API for: {url}")
        api_url = f"{settings.DOUYIN_API}/api/hybrid/video_data?url={url}"
        response = requests.get(api_url, timeout=30)
        # response.raise_for_status() # Don't raise checks yet, handle json first
        data = response.json()
        
        # Check success code
        if data.get('code') == 200:
            video_data = data.get('data', {})
            platform = video_data.get('platform')
            
            # A. BILIBILI HANDLER (Keep the robust fix)
            if platform == 'bilibili' or 'bilibili' in url or 'b23.tv' in url:
                 print("Bilibili detected via Hybrid/URL. Using specialized Bilibili fallback logic to ensure best quality.")
                 # Trigger the Bilibili specific logic below (in fallback section)
                 # Or just call it here? Let's just raise exception to trigger fallback 
                 # OR better, clear data so we go to fallback
                 raise Exception("Force Bilibili Fallback for best quality")

            # B. DOUYIN/TIKTOK HANDLER (Simpler success path from user snippet)
            # User's snippet: download_link = response['data']['video']['bit_rate'][0]['play_addr']['url_list'][0]
            # Verify structure exists
            
            # Hybrid response structure usually:
            # { data: { video_data: { ... }, ... } } 
            # BUT user snippet suggests: { data: { video: { bit_rate: ... } } }
            # Wait, the user's snippet used: api_url = .../api/hybrid/video_data
            # PROBABLY the hybrid generic response returns the raw downstream response?
            
            # Let's try to adapt to what the user said worked.
            # However, looking at my previous view, Hybrid returns: {'data': {'type': 'video', 'video_data': {'nwm_video_url': ...}}}
            
            # If the user says "old code worked", and old code used:
            # download_link = response['data']['video']['bit_rate'][0]['play_addr']['url_list'][0]
            # This implies the Hybrid API returns the raw Douyin/TikTok JSON structure in some cases? 
            # OR the "Hybrid" endpoint acts as a proxy pass-through?
            
            # Let's look at the Hybrid Crawler code to be sure... no I can't easily.
            # But I can stick to "If it looks like standard Hybrid, use it. If it fails, try the user's path".
            
            duration = video_data.get('duration', 0)
            
            # Try standard Hybrid fields first
            download_link = video_data.get('nwm_video_url_HQ') or video_data.get('nwm_video_url') or video_data.get('wm_video_url')
            
            # If standard fields fail, try the deep structure user provided (Just in case)
            if not download_link:
                 try:
                     download_link = video_data['video']['bit_rate'][0]['play_addr']['url_list'][0]
                 except:
                     pass

            if download_link:
                return {
                    'duration': duration,
                    'download_link': download_link,
                    'platform': platform,
                    'title': video_data.get('desc') or video_data.get('title')
                }
                
    except Exception as e:
        print(f"Hybrid/Standard API failed: {e}")

    # 2. Fallback / Specific Logic (For Bilibili or Failed Douyin)
    platform = None
    if 'douyin' in url: platform = 'douyin'
    elif 'tiktok' in url: platform = 'tiktok'
    elif 'bilibili' in url or 'b23.tv' in url: platform = 'bilibili'
    
    print(f"Fallback/Specialized logic detected platform: {platform}")

    if platform == 'bilibili':
        try:
             import re
             # Extract BV ID
             bv_match = re.search(r'(BV\w+)', url)
             if bv_match:
                 bvid = bv_match.group(1)
                 print(f"Extracted Bilibili BVID: {bvid}")
                 
                 data_api_base = settings.DOUYIN_API.rstrip('/')
                 info_url = f"{data_api_base}/api/bilibili/web/fetch_one_video?bv_id={bvid}"
                 print(f"Calling Bilibili Info API: {info_url}")
                 info_resp = requests.get(info_url, timeout=30).json()
                 
                 if info_resp.get('code') == 200:
                     info_data = info_resp.get('data', {}).get('data', {})
                     cid = info_data.get('cid')
                     title = info_data.get('title')
                     duration = info_data.get('duration', 0) * 1000
                     
                     if cid:
                         print(f"Got CID: {cid}. Fetching Play URL...")
                         play_url = f"{data_api_base}/api/bilibili/web/fetch_video_playurl?bv_id={bvid}&cid={cid}"
                         play_resp = requests.get(play_url, timeout=30).json()
                         
                         if play_resp.get('code') == 200:
                             play_data = play_resp.get('data', {}).get('data', {})
                             dash = play_data.get('dash', {})
                             durl = play_data.get('durl')
                             
                             if dash:
                                 # DASH HANDLING (Robust)
                                 video_streams = dash.get('video') or [] 
                                 audio_streams = dash.get('audio') or []
                                 if video_streams and audio_streams:
                                      v_stream = video_streams[0]
                                      a_stream = audio_streams[0]
                                      v_backups = v_stream.get('backupUrl') or []
                                      a_backups = a_stream.get('backupUrl') or []
                                      
                                      v_urls = [v_stream.get('baseUrl')] + v_backups
                                      a_urls = [a_stream.get('baseUrl')] + a_backups
                                      
                                      v_urls = [u for u in v_urls if u]
                                      a_urls = [u for u in a_urls if u]
                                      
                                      if v_urls and a_urls:
                                          return {
                                              'duration': duration,
                                              'download_links': v_urls,
                                              'audio_download_links': a_urls,
                                              'platform': 'bilibili',
                                              'title': title,
                                              'is_dash': True
                                          }
                             if durl:
                                 stream = durl[0]
                                 dl_links = [stream.get('url')] + stream.get('backup_url', [])
                                 dl_links = [u for u in dl_links if u]
                                 return {
                                     'duration': duration,
                                     'download_links': dl_links,
                                     'platform': 'bilibili',
                                     'title': title,
                                     'is_dash': False
                                 }
        except Exception as e:
            print(f"Bilibili Fallback failed: {e}")
            
    # Raise exception if everything fails
    raise Exception("All download methods failed.")

def process_download(message_body: bytes):
    video_id = None
    temp_original_path = None
    
    try:
        data = json.loads(message_body)
        video_id = data.get('videoId')
        url = data.get('url')
        
        print(f"Processing Download: {video_id} - {url}")

        # 1. Fetch Video Info (Robust)
        try:
            video_info = fetch_video_info(url)
        except Exception as e:
             print(f"Download failed for {video_id}: {e}")
             if video_id:
                video_collection.update_one(
                    {'_id': ObjectId(video_id)}, 
                    {'$set': {'status': 'failed', 'errorMsg': f'Failed to fetch video info: {str(e)}'}}
                )
             return

        # Unwrap info
        duration_ms = video_info.get('duration', 0)
        download_link = video_info.get('download_link')
        download_links = video_info.get('download_links')
        
        display_link = "List: " + str(download_links) if download_links else (download_link or "None")
        print(f"Video Info Fetched: Duration={duration_ms}ms, Link={display_link[:100]}...")
        
        # 1.5 Check User Balance
        print(f"Checking database for videoId: {video_id}...")
        video_doc = video_collection.find_one({'_id': ObjectId(video_id)})
        if not video_doc:
            print(f"Error: Video document {video_id} not found in database!")
            return
        
        user_id = video_doc.get('userId')
        print(f"Fetching user balance for user_id: {user_id}...")
        user = user_collection.find_one({'_id': ObjectId(user_id)})
        
        if user:
            remaining_time = user.get('remaining_time_ms', 0)
            print(f"User Balance: {remaining_time}ms, Required: {duration_ms}ms")
            if remaining_time < duration_ms:
                print(f"User {user_id} insufficient balance. Required: {duration_ms}, Remaining: {remaining_time}")
                video_collection.update_one(
                    {'_id': ObjectId(video_id)}, 
                    {'$set': {'status': 'failed', 'errorMsg': f'Insufficient balance. Required: {duration_ms/1000}s, Remaining: {remaining_time/1000}s'}}
                )
                return

        duration_minutes = duration_ms / 1000 / 60
        max_duration_minutes = int(settings.MAX_DURATION_MINUTES)
        if duration_minutes > max_duration_minutes:
            print(f"Video {video_id} too long ({duration_minutes} mins). Rejected.")
            video_collection.update_one(
                {'_id': ObjectId(video_id)}, 
                {'$set': {'status': 'rejected', 'errorMsg': f'Video duration exceeds {max_duration_minutes} minutes limit.'}}
            )
            return

        # 2. Tải video về file tạm
        # Update headers if needed (Bilibili often needs Referer)
        headers = {}
        if 'bilibili' in (video_info.get('platform') or ''):
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.bilibili.com/',
                'Origin': 'https://www.bilibili.com',
                'Sec-Fetch-Dest': 'video',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive'
            }

        # Helper to download with retries/backups
        def download_with_fallback(urls, dest_path, headers):
            if not urls: return False
            if isinstance(urls, str): urls = [urls]
            for i, link in enumerate(urls):
                try:
                    print(f"Downloading from URL {i+1}/{len(urls)}: {link[:50]}...")
                    resp = requests.get(link, headers=headers, stream=True, timeout=30)
                    resp.raise_for_status()
                    with open(dest_path, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=1024*1024):
                            if chunk: f.write(chunk)
                    return True
                except Exception as e:
                    print(f"Failed to download from URL {i+1}: {e}")
            return False

        # Main Video Download
        # Unify into a list
        download_links = video_info.get('download_links') 
        if not download_links:
             single_link = video_info.get('download_link')
             if single_link:
                 download_links = [single_link]
             else:
                 download_links = []
                 
        fd, temp_download_path = tempfile.mkstemp(suffix=".mp4")
        os.close(fd)
        
        if not download_with_fallback(download_links, temp_download_path, headers):
            raise Exception("Failed to download video from all provided URLs.")

        # 2.5 Handle DASH (Merge Audio if present)
        audio_download_links = video_info.get('audio_download_links')
        if not audio_download_links:
             single_audio = video_info.get('audio_download_link')
             if single_audio:
                 audio_download_links = [single_audio]

        if audio_download_links:
            print(f"DASH detected, downloading separate audio stream...")
            fd_a, temp_audio_path = tempfile.mkstemp(suffix=".mp4")
            os.close(fd_a)
            
            try:
                if download_with_fallback(audio_download_links, temp_audio_path, headers):
                    # Merge
                    fd_m, temp_merged_path = tempfile.mkstemp(suffix=".mp4")
                    os.close(fd_m)
                    
                    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
                    print("Merging video and audio...")
                    
                    # ffmpeg -i v.mp4 -i a.mp4 -c:v copy -c:a copy merged.mp4
                    subprocess.run([
                        ffmpeg_exe, '-y',
                        '-i', temp_download_path,
                        '-i', temp_audio_path,
                        '-c:v', 'copy',
                        '-c:a', 'copy',
                        temp_merged_path
                    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    
                    # Cleanup and Swap
                    if os.path.exists(temp_download_path): os.remove(temp_download_path)
                    
                    temp_download_path = temp_merged_path
                    print("Merge complete.")
                else:
                    print("Failed to download audio stream. Proceeding with video only.")
                    
            except Exception as e:
                print(f"Error merging DASH audio: {e}. Proceeding with video only.")
            finally:
                if os.path.exists(temp_audio_path): os.remove(temp_audio_path)
        
        # 2.1. Speed Up Video (Configurable via SPEED_RATE in .env)
        # User Request: "Khi vừa tải video về hãy mặc định SPEED_RATE tốc độ"
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        speed_rate = float(settings.SPEED_RATE)
        
        if speed_rate != 1.0:  # Only apply if speed rate is different from 1.0
            fd_speed, temp_speedup_path = tempfile.mkstemp(suffix=".mp4")
            os.close(fd_speed)
            
            print(f"Speeding up video {video_id} by {speed_rate}x...")
            try:
                subprocess.run([
                    ffmpeg_exe, '-y',
                    '-i', temp_download_path,
                    '-filter_complex', f'[0:v]setpts=PTS/{speed_rate}[v];[0:a]atempo={speed_rate}[a]',
                    '-map', '[v]', '-map', '[a]',
                    temp_speedup_path
                ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                # Remove original download, switch variable
                os.remove(temp_download_path)
                temp_original_path = temp_speedup_path
                
                # Update duration_ms reflecting speed change
                duration_ms = int(duration_ms / speed_rate)
                
            except subprocess.CalledProcessError as e:
                print(f"Error speeding up video, using original: {e}")
                temp_original_path = temp_download_path  # Fallback
        else:
            temp_original_path = temp_download_path
        
        # 3. Xử lý Video bằng FFmpeg (Optimized)
        audio_keys = []
        events = []
        mute_key = ""
        sample_key = ""
        
        # Lấy đường dẫn ffmpeg từ thư viện (an toàn hơn là gọi 'ffmpeg' global)
        
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        
        # A. Tạo Sample 1 phút
        # Nếu video <= 1 phút, dùng chính nó làm sample luôn để tránh lỗi ffmpeg stream copy
        sample_key = f"raw/{video_id}/sample.mp4"
        
        if duration_ms <= 60000:
            print(f"Video short ({duration_ms}ms), utilizing original as sample.")
            with open(temp_original_path, 'rb') as f_origin:
                file_stat = os.stat(temp_original_path)
                minio_client.put_object(
                    settings.MINIO_BUCKET,
                    sample_key,
                    f_origin,
                    file_stat.st_size,
                    content_type="video/mp4"
                )
        else:
            # Video dài > 1 phút, cắt 60 giây đầu
            fd_sample, sample_path = tempfile.mkstemp(suffix=".mp4")
            os.close(fd_sample)
            
            try:
                subprocess.run([
                    ffmpeg_exe, '-y',
                    '-i', temp_original_path,
                    '-ss', '0',
                    '-t', '60',
                    '-c', 'copy', # Stream copy
                    sample_path
                ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                with open(sample_path, 'rb') as f_sample:
                    file_stat = os.stat(sample_path)
                    minio_client.put_object(
                        settings.MINIO_BUCKET,
                        sample_key,
                        f_sample,
                        file_stat.st_size,
                        content_type="video/mp4"
                    )
            except subprocess.CalledProcessError as e:
                print(f"Error creating sample: {e}")
            finally:
                if os.path.exists(sample_path):
                    os.remove(sample_path)
        
        # B. Tạo Mute Video (Stream Copy & Remove Audio)
        # Lệnh: ffmpeg -i input.mp4 -c copy -an -y mute.mp4
        fd_mute, mute_path = tempfile.mkstemp(suffix=".mp4")
        os.close(fd_mute)
        
        try:
            subprocess.run([
                ffmpeg_exe, '-y',
                '-i', temp_original_path,
                '-c', 'copy', # Copy video stream
                '-an',        # No Audio
                mute_path
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            mute_key = f"raw/{video_id}/origin_mute.mp4"
            with open(mute_path, 'rb') as f_mute:
                file_stat = os.stat(mute_path)
                minio_client.put_object(
                    settings.MINIO_BUCKET,
                    mute_key,
                    f_mute,
                    file_stat.st_size,
                    content_type="video/mp4"
                )
        except subprocess.CalledProcessError as e:
            print(f"Error creating mute video: {e}")
        finally:
            if os.path.exists(mute_path):
                os.remove(mute_path)
        
        # C. Tách Audio thành chunks (Max CHUNK_DURATION_MINUTES mins)
        # Lệnh: ffmpeg -i input.mp4 -f segment -segment_time 300 -c:a libmp3lame -vn -y audio_%d.mp3
        # Tạo thư mục tạm để chứa các file audio chunks
        chunk_duration_minutes = int(settings.CHUNK_DURATION_MINUTES)
        chunk_duration_seconds = chunk_duration_minutes * 60
        
        with tempfile.TemporaryDirectory() as temp_audio_dir:
            audio_pattern = os.path.join(temp_audio_dir, "audio_%03d.mp3")    
            print(f"Splitting audio for {video_id}...")
            try:
                subprocess.run([
                    ffmpeg_exe, '-y',
                    '-i', temp_original_path,
                    '-f', 'segment',
                    '-segment_time', str(chunk_duration_seconds), # Dynamic duration
                    '-c:a', 'libmp3lame',   # Convert sang mp3
                    '-vn',                  # Bỏ video
                    audio_pattern
                ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                # Upload các file chunks lên MinIO
                for filename in os.listdir(temp_audio_dir):
                    if filename.endswith(".mp3"):
                        local_file_path = os.path.join(temp_audio_dir, filename)
                        # Parse index từ filename audio_000.mp3
                        try:
                            file_index = int(filename.split('_')[1].split('.')[0])
                        except ValueError:
                            file_index = 0
                            
                        # Get Audio Chunk Duration
                        chunk_duration_ms = 0
                        try:
                            # Try to find ffprobe
                            ffprobe_exe = ffmpeg_exe.replace('ffmpeg.exe', 'ffprobe.exe').replace('ffmpeg', 'ffprobe')
                            if not os.path.exists(ffprobe_exe):
                                # Fallback to system path
                                ffprobe_exe = 'ffprobe'
                            
                            # print(f"Using ffprobe: {ffprobe_exe}")

                            result = subprocess.run(
                                [ffprobe_exe, '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', local_file_path],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT
                            )
                            chunk_duration_ms = int(float(result.stdout) * 1000)
                        except Exception as e:
                            print(f"Error checking duration for chunk {filename}: {e}")

                        is_last_chunk = False
                        if file_index == len(os.listdir(temp_audio_dir)) - 1:
                            is_last_chunk = True   
                            
                        s3_key = f"raw/{video_id}/audio_part_{file_index}.mp3"
                        
                        with open(local_file_path, 'rb') as f_chunk:
                            file_stat = os.stat(local_file_path)
                            minio_client.put_object(
                                settings.MINIO_BUCKET,
                                s3_key,
                                f_chunk,
                                file_stat.st_size,
                                content_type="audio/mpeg"
                            )
                        audio_keys.append({
                            'audio_key': s3_key,
                            'file_index': file_index,
                            'duration_ms': chunk_duration_ms,
                            'srt_output' : ""
                        })
                        # each audio file create one record audio (collection audios)
                        audio_doc = {
                            'video_id': video_id,
                            'audio_key': s3_key,
                            'file_index': file_index,
                            'duration_ms': chunk_duration_ms,
                            'status': 'pending',
                            'is_last_chunk': is_last_chunk
                        }
                        result = audio_collection.insert_one(audio_doc)
                        # Publish event to rabbit to topic topic_ai_process
                        events.append({
                            'audio_id': str(result.inserted_id),
                            'video_id': video_id,
                            'audio_key': s3_key,
                            'file_index': file_index
                        })
                # Sort lại key cho đúng thứ tự (quan trọng)
                audio_keys.sort(key=lambda x: x['file_index'])
                
            except subprocess.CalledProcessError as e:
                print(f"Error splitting audio: {e}")

        # 4. Update Database
        # First, get video document to check if auto_edit or batch_edit is enabled
        video_doc = video_collection.find_one({'_id': ObjectId(video_id)})
        auto_edit = video_doc.get('auto_edit', False) if video_doc else False
        batch_edit = video_doc.get('batch_edit', False) if video_doc else False
        
        video_collection.update_one(
            {'_id': ObjectId(video_id)}, 
            {'$set': {
                'stage': 'user_editing', 
                'status': 'pending', 
                'chunk_duration_minutes': chunk_duration_minutes,
                'chunk_duration_ms': audio_keys[0]['duration_ms'],
                'sub': {
                    'audios': audio_keys,
                    'sample': sample_key,
                    'video_origin_mute': mute_key,
                    'total_audio_parts': len(audio_keys),
                    'srt_concatenated': ""
                },
                'original_duration_ms': duration_ms
            }}
        )

        # 6. Double Check Balance & Deduct (Critical for Race Condition)
        # Use Atomic find_one_and_update to ensure consistency
        print(f"Final Atomic Balance Check & Deduct for User {user_id}...")
        
        try:
            # Atomic: Check if balance >= duration AND deduct in one go
            updated_user = user_collection.find_one_and_update(
                {
                    '_id': ObjectId(user_id),
                    'remaining_time_ms': {'$gte': duration_ms}
                },
                {
                    '$inc': {'remaining_time_ms': -duration_ms}
                },
                return_document=True # Return the NEW document (after update) to confirm
            )
            
            if not updated_user:
                # If None, it means the query failed (likely insufficient balance)
                # Fetch current balance just for logging (non-critical)
                current_user = user_collection.find_one({'_id': ObjectId(user_id)})
                current_balance = current_user.get('remaining_time_ms', 0) if current_user else 0
                
                print(f"CRITICAL: Atomic deduction failed. Insufficient balance. User: {current_balance}, Required: {duration_ms}. Rolling back.")
                
                # ROLLBACK: Delete all MinIO keys
                try:
                    # 1. Delete Sample
                    if sample_key:
                        minio_client.remove_object(settings.MINIO_BUCKET, sample_key)
                    # 2. Delete Mute
                    if mute_key:
                        minio_client.remove_object(settings.MINIO_BUCKET, mute_key)
                    # 3. Delete Audio Chunks
                    for ak in audio_keys:
                        if ak.get('audio_key'):
                            minio_client.remove_object(settings.MINIO_BUCKET, ak['audio_key'])
                            
                    print(f"Rollback cleanup complete for video {video_id}")
                except Exception as e_cleanup:
                     print(f"Error during rollback cleanup: {e_cleanup}")

                # Update Video Status Failed
                video_collection.update_one(
                    {'_id': ObjectId(video_id)}, 
                    {'$set': {'status': 'failed', 'errorMsg': f'Insufficient balance during processing. Required: {duration_ms/1000}s'}}
                )
                return
            
            # If we are here, deduction succeeded
            new_remaining = updated_user.get('remaining_time_ms')
            print(f"Deducted {duration_ms}ms from User {user_id}. New balance: {new_remaining}ms")

        except Exception as e_db:
             print(f"Database error during atomic deduction: {e_db}")
             return

        # 5. Check if auto_edit or batch_edit is enabled (Moved after deduction)
        # If yes, automatically publish to AI queue (skip manual user editing)
        if auto_edit or batch_edit:
            print(f"Auto-edit/Batch-edit enabled for {video_id}, auto-publishing to AI queue...")
            
            # Update stage to ai_process and status to inprogress
            video_collection.update_one(
                {'_id': ObjectId(video_id)}, 
                {'$set': {
                    'stage': 'ai_process',
                    'status': 'inprogress'
                }}
            )
            
            # Publish events to RabbitMQ for each audio chunk
            for event in events:
                publish_event('topic_ai_process', event)
            
            print(f"Auto-queued {len(events)} audio chunks for video {video_id}")
        else:
            # Normal flow: wait for user to manually edit
            print(f"Download & Processing Complete: {video_id} - Ready for user editing")
        
    except Exception as e:
        print(f"Error processing download: {e}")
        if video_id:
             video_collection.update_one({'_id': ObjectId(video_id)}, {'$set': {'status': 'failed', 'errorMsg': "Error during download"}})
    
    finally:
        if temp_original_path and os.path.exists(temp_original_path):
            try:
                os.remove(temp_original_path)
            except OSError:
                pass



def process_edit(message_body: bytes):
    try:
        data = json.loads(message_body)
        video_id = data.get('videoId')
        print(f"Processing Edit: {video_id}")
        
        video_doc = video_collection.find_one({'_id': ObjectId(video_id)})
        if not video_doc: return
            
        user_id = video_doc.get('userId')
        sub_data = video_doc.get('sub', {})
        video_mute_key = sub_data.get('video_origin_mute')
        audio_key = sub_data.get('final_audio_key')
        srt_content = sub_data.get('srt_concatenated', '')
        
        meta = video_doc.get('user_editing_meta', {})
        resize_mode = meta.get('resize_mode', '9:16')
        logo_config = meta.get('logo', {})
        sub_config = meta.get('subtitle', {})
        
        # 1. Khởi tạo FFmpeg
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        # Tìm ffprobe (nằm cùng thư mục với ffmpeg)
        # Try different potential names for ffprobe
        ffprobe_exe = ffmpeg_exe.replace("ffmpeg.exe", "ffprobe.exe").replace("ffmpeg", "ffprobe")
        if not os.path.exists(ffprobe_exe):
            print(f"WARNING: ffprobe not found at {ffprobe_exe}, falling back to system PATH")
            ffprobe_exe = "ffprobe" # Hope it's in PATH
        
        temp_files = []
        
        def safe_path_for_ffmpeg(p: str) -> str:
            # Strictly use forward slashes, no other escaping (as per test.py)
            return str(Path(p).resolve()).replace("\\", "/")

        def hex_to_ass_style(hex_color):
            if not hex_color: return "&H00FFFFFF&"
            hex_color = hex_color.lstrip('#')
            if len(hex_color) == 6:
                r, g, b = hex_color[0:2], hex_color[2:4], hex_color[4:6]
                # Return 8-digit hex (00 for opaque alpha)
                return f"&H00{b}{g}{r}&" 
            return "&H00FFFFFF&"

        try:
            # 2. Download tài nguyên
            # ... (download logic unchanged) ...
            fd_video, video_path = tempfile.mkstemp(suffix=".mp4"); os.close(fd_video); temp_files.append(video_path)
            with minio_client.get_object(settings.MINIO_BUCKET, video_mute_key) as obj:
                with open(video_path, 'wb') as f:
                    for chunk in obj.stream(32*1024): f.write(chunk)
                        
            fd_audio, audio_path = tempfile.mkstemp(suffix=".mp3"); os.close(fd_audio); temp_files.append(audio_path)
            with minio_client.get_object(settings.MINIO_BUCKET, audio_key) as obj:
                with open(audio_path, 'wb') as f:
                    for chunk in obj.stream(32*1024): f.write(chunk)
                 
            fd_srt, srt_path = tempfile.mkstemp(suffix=".srt"); os.close(fd_srt); temp_files.append(srt_path)
            with open(srt_path, 'w', encoding='utf-8') as f:
                f.write(srt_content if srt_content else "")

            # 3. Lấy kích thước Video bằng FFPROBE (Thay thế MoviePy)
            try:
                probe_cmd = [
                    ffprobe_exe, "-v", "error", "-select_streams", "v:0",
                    "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0", video_path
                ]
                print(f"DEBUG: Probing video with: {probe_cmd}")
                probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
                if probe_result.returncode != 0:
                     print(f"Probe failed stderr: {probe_result.stderr}")
                     raise Exception("ffprobe returned non-zero")
                     
                # Kết quả trả về dạng "1080x1920"
                vid_w, vid_h = map(int, probe_result.stdout.strip().split('x'))
                print(f"DEBUG: Detected Dims: {vid_w}x{vid_h}")
            except Exception as e:
                print(f"Probe failed: {e}. Defaulting to 1080x1920")
                vid_w, vid_h = 1080, 1920 # Fallback

        # 4. Chuẩn bị Inputs & Filters (Sử dụng ffmpeg-python)
            inputs = []
            
            print(f"DEBUG: ffmpeg module: {ffmpeg_lib}")
            try:
                print(f"DEBUG: ffmpeg module path: {ffmpeg_lib.__file__}")
            except:
                print("DEBUG: ffmpeg module has no __file__")

            if not hasattr(ffmpeg_lib, 'input'):
                 print(f"ERROR: ffmpeg module missing 'input'. dir: {dir(ffmpeg_lib)}")
            
            # Input 0: Video
            in_v = ffmpeg_lib.input(video_path)
            v = in_v.video
            
            # Input 1: Audio
            in_a = ffmpeg_lib.input(audio_path)
            a = in_a.audio

            pad_v = 0
            
            # A. Resize/Crop
            if resize_mode == '9:16':
                 target_w = int(vid_h * (9/16))
                 if vid_w > target_w:
                      v = v.crop(x='(iw-ow)/2', y=0, width=target_w, height=vid_h)
                      vid_w = target_w
            elif resize_mode == '16:9':
                 target_h = int(vid_w * (9/16))
                 if vid_h > target_h:
                      # crop=w:h:x:y
                      v = v.crop(x=0, y='(ih-oh)/2', width=vid_w, height=target_h)
                      vid_h = target_h

            # Common Scaling Logic
            resize_mode = meta.get('resize_mode', '9:16')
            canvas_w = 360 if resize_mode == '9:16' else 640
            canvas_h = 640 if resize_mode == '9:16' else 360
            
            scale_x = vid_w / canvas_w
            scale_y = vid_h / canvas_h
            
            print(f"DEBUG - Canvas: {canvas_w}x{canvas_h}, Video: {vid_w}x{vid_h}")
            print(f"DEBUG - Scale Factors: x={scale_x:.2f}, y={scale_y:.2f}")

            def convert_px_to_v(px,height=vid_h):
                rate_V_px = height / 275
                return px / rate_V_px

            def calculate_font_size(raw_size,resize_mode):
                size_v = raw_size
                if resize_mode == '9:16':
                    size_v = size_v // 3.5
                else:
                    size_v = size_v // 2
                return size_v
            # B. Subtitles
            if srt_content and srt_content.strip():
                # Define helper for color
                def hex_to_ass_color(hex_str):
                    if not hex_str or hex_str == 'transparent':
                        return None
                    hex_str = hex_str.lstrip('#')
                    r, g, b = hex_str[0:2], hex_str[2:4], hex_str[4:6]
                    return f"&H00{b}{g}{r}&"

                # Get Config (Pixel Values from Frontend)
                sub_x = float(sub_config.get('x', 0))
                sub_y = float(sub_config.get('y', vid_h * 0.8)) # Fallback if missing
                sub_w = float(sub_config.get('width', 300))
                sub_h = float(sub_config.get('height', 50))
                raw_font_size = float(sub_config.get('font_size', 24))
                
                custom_scale = 0.8
                custom_pad_v = 0
                if resize_mode == "16:9":
                    custom_scale = 0.8
                    custom_pad_v = -2

                # Scale to Actual Video
                real_x = int(sub_x * scale_x )
                real_y = int(sub_y * scale_y  )
                real_w = int(sub_w * scale_x )
                real_h = int(sub_h * scale_y * custom_scale)                
                real_font_size = calculate_font_size(raw_font_size, resize_mode)
                
                # Colors
                text_color_hex = sub_config.get('color', '#FFFFFF')
                bg_color_hex = sub_config.get('bg_color', '#000000')
                text_color_ass = hex_to_ass_color(text_color_hex) or "&H00FFFFFF&"
                
                # 1. DRAW BACKGROUND BOX (First Layer)
                if bg_color_hex and bg_color_hex != 'transparent':
                    v = v.drawbox(
                        x=real_x, y=real_y, 
                        width=real_w, height=real_h, 
                        color=bg_color_hex, t="fill"
                    )

                # 2. SUBTITLES (Second Layer - On Top)

                style = (
                        "FontName=Arial,"
                        f"Fontsize={real_font_size},"
                        f"PrimaryColour={text_color_ass},"
                        "BorderStyle=1,"
                        "Outline=0.1,"
                        "Shadow=0,"
                        f"MarginL=0,"
                        f"MarginV={convert_px_to_v(vid_h - real_y - real_h * 0.6 + custom_pad_v)}"
                    )

                # 1280 px = 275V +- 3V
                
                print(f"DEBUG - Subtitle Box: x={real_x}, y={real_y}, w={real_w}, h={real_h}")
                
                srt_path_fixed = safe_path_for_ffmpeg(srt_path)
                v = v.filter('subtitles', filename=srt_path_fixed, charenc='UTF-8', force_style=style)

            # C. Logo (User Custom Logo)
            if logo_config and logo_config.get('file_key'):
                logo_key = logo_config.get('file_key')
                fd_logo, logo_path = tempfile.mkstemp(suffix=".png"); os.close(fd_logo); temp_files.append(logo_path)
                try:
                    with minio_client.get_object(settings.MINIO_BUCKET, logo_key) as obj:
                        with open(logo_path, 'wb') as f:
                            for chunk in obj.stream(32*1024): f.write(chunk)
                    
                    # 1. Exact Dimensions from Frontend (Scaled)
                    l_w = float(logo_config.get('width'))
                    l_h = float(logo_config.get('height'))
                    target_logo_w = int(l_w * scale_x)
                    target_logo_h = int(l_h * scale_y)
                        
                    # Position (Pixel)
                    logo_x_px = float(logo_config.get('x', 0))
                    logo_y_px = float(logo_config.get('y', 0))
                    actual_x = int(logo_x_px * scale_x)
                    actual_y = int(logo_y_px * scale_y)
                        
                    # Scale (Allow Distortion to match Background Bar logic)
                    logo_v = ffmpeg_lib.input(logo_path).filter('scale', w=target_logo_w, h=target_logo_h)
                    
                    # Apply Overlay
                    v = v.overlay(logo_v, x=actual_x, y=actual_y)
                    
                except Exception as e:
                    print(f"Error processing logo: {e}")

            # D. Free User Watermark (Forced Branding)
            # Check user total deposited
            try:
                user_doc = user_collection.find_one({'_id': ObjectId(user_id)})
                total_deposited = user_doc.get('total_deposited_vnd', 0) if user_doc else 0
                
                if total_deposited <= 0:
                    print(f"Free user detected (Deposit: {total_deposited}). Adding forced watermark.")
                    watermark_path = "/app/logo.png" # Path in Docker
                    
                    if os.path.exists(watermark_path):
                        # Add watermark
                        wm_in = ffmpeg_lib.input(watermark_path)
                        # Scale to 30% width, keep aspect ratio
                        # w=video_width*0.3
                        # We use 'ivw' (input video width) if possible, or use fixed vid_w
                        
                        # Note: We need to ensure we use the 'v' stream dims. 
                        # ffmpeg-python filter string can take expressions?
                        # scale=w=iw*0.3:h=-1
                        # But wait, 'iw' refers to the input of the filter (the logo itself).
                        # We want 30% of main video.
                        # We know vid_w is the main video width.
                        
                        wm_target_w = int(vid_w * 0.3)
                        wm_scaled = wm_in.filter('scale', w=wm_target_w, h=-1)
                        
                        # Overlay at center: x=(W-w)/2:y=(H-h)/2
                        v = v.overlay(wm_scaled, x="(W-w)/2", y="(H-h)/2")
                    else:
                        print(f"WARNING: Watermark file not found at {watermark_path}")

            except Exception as e_wm:
                print(f"Error processing free watermark: {e_wm}")

            # 5. Build & Run Command
            fd_final, final_path = tempfile.mkstemp(suffix=".mp4"); os.close(fd_final); temp_files.append(final_path)
            
            print(f"DEBUG - Running ffmpeg-python command...")
            
            # Global args (inputs are already defined in fluent chain)
            # Output arguments
            out_args = {
                'vcodec': 'libx264',
                'preset': 'ultrafast',
                'crf': '23',
                'pix_fmt': 'yuv420p',
                'acodec': 'aac',
                'audio_bitrate': '192k',
                'shortest': None
            }
            
            # Compile and run
            # Important: pass cmd=ffmpeg_exe to use the binary we found
            (
                ffmpeg_lib
                .output(v, a, final_path, **out_args)
                .overwrite_output()
                .run(cmd=ffmpeg_exe, quiet=False)
            )
            
            print(f"Running ffmpeg command SUCCESS")
            
            # 6. Upload
            final_s3_key = f"result/{user_id}/{video_id}.mp4"
            with open(final_path, 'rb') as f_final:
                minio_client.put_object(settings.MINIO_BUCKET, final_s3_key, f_final, os.stat(final_path).st_size, "video/mp4")
            
            video_collection.update_one({'_id': ObjectId(video_id)}, {'$set': {'status': 'completed', 'stage': 'completed', 'download_link': final_s3_key}})
            
            # 7. Cleanup Raw Files from MinIO (To save SSD)
            print(f"Cleaning up raw files for {video_id}...")
            try:
                # Keys to delete
                keys_to_delete = []
                if video_mute_key: keys_to_delete.append(video_mute_key) # origin_mute
                if audio_key: keys_to_delete.append(audio_key)           # final_audio
                
                # Sample
                sample_key = sub_data.get('sample')
                if sample_key: keys_to_delete.append(sample_key)
                
                # Audio Chunks
                audios = sub_data.get('audios', [])
                for a in audios:
                    if a.get('audio_key'):
                         keys_to_delete.append(a.get('audio_key'))
                
                for k in keys_to_delete:
                    try:
                        minio_client.remove_object(settings.MINIO_BUCKET, k)
                    except Exception as e_del:
                        print(f"Failed to delete {k}: {e_del}")
                        
                print(f"Cleanup finished. Deleted {len(keys_to_delete)} objects.")
                
            except Exception as e_cleanup:
                print(f"Error during post-processing cleanup: {e_cleanup}")


        finally:
            for p in temp_files:
                if os.path.exists(p): os.remove(p)

    except Exception as e:
        print(f"Error: {e}")
        if video_id: video_collection.update_one({'_id': ObjectId(video_id)}, {'$set': {'status': 'failed', 'errorMsg': str(e)}})