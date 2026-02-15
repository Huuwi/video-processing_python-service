import os
import math
import numpy as np
import torch
import torchaudio
import soundfile as sf
import ffmpeg
from pathlib import Path

# =========================
# CẤU HÌNH (SỬA TẠI ĐÂY)
# =========================
INPUT_MP4 = r"D:\work\test\original_audio\daubuoi.mp4"
INPUT_MP3 = r"D:\work\test\output_mp3.mp3"
SRT_PATH  = r"D:\work\test\output_vn.srt"

WORK_DIR   = r"D:\work\test\final"
OUTPUT_MP4 = r"D:\work\test\final\output_final.mp4"

# Audio out
AUDIO_CODEC   = "aac"
AUDIO_BITRATE = "192k"
TARGET_SR     = 44100   # chuẩn hoá về 44.1kHz

# Mix levels (dB)
BGM_REDUCE_DB   = -10.0   # giảm nhạc nền sau khi tách vocal
MP3_GAIN_DB     = 0.0     # khuếch đại/giảm file MP3 chèn thêm
MASTER_GAIN_DB  = 0.0     # gain toàn track sau khi trộn (0 = giữ nguyên)

# =========================
# TIỆN ÍCH
# =========================
def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)

def check_file_exists(p: str, desc: str):
    if not Path(p).exists():
        raise FileNotFoundError(f"[ERR] Không tìm thấy {desc}: {p}")

def db_to_linear(db):
    return 10.0 ** (db / 20.0)

def safe_path_for_ffmpeg(p: str) -> str:
    # Tuyệt đối + forward-slash để tránh lỗi escape trên Windows
    return str(Path(p).resolve()).replace("\\", "/")

def load_audio_resample_to_stereo(path, target_sr=TARGET_SR, device="cpu"):
    wav, sr = torchaudio.load(path)
    # Mono -> Stereo
    if wav.shape[0] == 1:
        wav = wav.repeat(2, 1)
    # Resample nếu cần
    if sr != target_sr:
        resampler = torchaudio.transforms.Resample(orig_freq=sr, new_freq=target_sr).to(device)
        wav = resampler(wav.to(device)).cpu()
        sr = target_sr
    return wav, sr

def extract_audio_from_mp4(mp4_path, wav_out_path, target_sr=TARGET_SR):
    (
        ffmpeg
        .input(mp4_path)
        .output(
            wav_out_path,
            ac=2, ar=target_sr,  # stereo + sample rate
            format='wav',
            vn=None
        )
        .overwrite_output()
        .run(quiet=True)
    )

def save_wav(path, wav_tensor, sr):
    wav_np = wav_tensor.detach().cpu().numpy().T  # [time, ch]
    sf.write(path, wav_np, sr, subtype='PCM_16')

def loop_to_length(x: np.ndarray, length_samples: int):
    if x.shape[0] >= length_samples:
        return x[:length_samples]
    reps = math.ceil(length_samples / x.shape[0])
    y = np.vstack([x] * reps)
    return y[:length_samples]

# =========================
# DEMUCS (GPU)
# =========================
def separate_vocals_demucs(wav_tensor, sr, device="cuda"):
    """
    Trả về dict: {'vocals', 'bass', 'drums', 'other'}, mỗi cái [ch, time]
    """
    from demucs.pretrained import get_model
    from demucs.apply import apply_model

    model = get_model(name="htdemucs")
    model.to(device)
    model.eval()

    with torch.no_grad():
        sources = apply_model(
            model,
            wav_tensor.unsqueeze(0).to(device),  # [1, ch, time]
            shifts=1,
            split=True,
            overlap=0.25
        )[0].cpu()  # [sources, ch, time]

    stems = {}
    for i, name in enumerate(model.sources):
        stems[name] = sources[i]
    return stems

# =========================
# QUY TRÌNH CHÍNH
# =========================
def main():
    ensure_dir(WORK_DIR)

    # Kiểm tra file đầu vào tồn tại – lỗi của bạn trước đó là SRT không tồn tại
    check_file_exists(INPUT_MP4, "video MP4")
    check_file_exists(INPUT_MP3, "audio MP3 chèn thêm")
    check_file_exists(SRT_PATH,  "phụ đề SRT")  # nếu chưa có, sửa đúng đường dẫn hoặc copy file vào

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    # 1) Tách audio từ MP4
    src_wav_path = str(Path(WORK_DIR) / "video_audio_src.wav")
    print(">> Tách audio từ MP4...")
    extract_audio_from_mp4(INPUT_MP4, src_wav_path, target_sr=TARGET_SR)

    # 2) Demucs tách vocal
    print(">> Load WAV và tách vocals bằng Demucs...")
    wav, sr = load_audio_resample_to_stereo(src_wav_path, target_sr=TARGET_SR, device=device)
    stems = separate_vocals_demucs(wav, sr, device=device)

    vocals = stems["vocals"]
    bass   = stems["bass"]
    drums  = stems["drums"]
    other  = stems["other"]

    # 3) Nhạc nền = bass + drums + other (loại vocals) + giảm dB
    accompaniment = (bass + drums + other + vocals*db_to_linear(-6)) * db_to_linear(BGM_REDUCE_DB)

    # 4) Load MP3 và trộn (KHÔNG lặp MP3 nếu ngắn)
    print(">> Load MP3 ngoài và trộn vào nhạc nền (không lặp)...")
    mp3_wav, _ = load_audio_resample_to_stereo(INPUT_MP3, target_sr=TARGET_SR, device=device)

    acc_np = accompaniment.detach().cpu().numpy().T  # [time, ch]
    mp3_np = mp3_wav.detach().cpu().numpy().T       # [time, ch]

    # Áp gain cho MP3 nếu muốn
    mp3_np = mp3_np * db_to_linear(MP3_GAIN_DB)

    # Trộn theo phần overlap
    min_len = min(acc_np.shape[0], mp3_np.shape[0])
    mixed_head = acc_np[:min_len] + mp3_np[:min_len]

    # Phần còn lại:
    # - Nếu MP3 ngắn hơn: nối phần nhạc nền còn lại, KHÔNG lặp MP3
    # - Nếu MP3 dài hơn: cắt MP3 dư (không cần làm gì thêm vì đã lấy min_len)
    if acc_np.shape[0] > min_len:
        mixed_np = np.vstack([mixed_head, acc_np[min_len:]])
    else:
        mixed_np = mixed_head

    mixed_tensor = torch.from_numpy(mixed_np.T).float()

    # 5) Lưu WAV đã mix
    mixed_wav_path = str(Path(WORK_DIR) / "audio_mixed_no_vocals_plus_mp3.wav")
    print(">> Lưu audio mix tạm:", mixed_wav_path)
    save_wav(mixed_wav_path, mixed_tensor, sr=TARGET_SR)

    # 6) Ghép audio + burn phụ đề SRT
    print(">> Ghép audio mới + chèn phụ đề SRT...")

    in_v = ffmpeg.input(INPUT_MP4)
    in_a = ffmpeg.input(mixed_wav_path)
    final_audio = in_a.audio

    # Style ASS: trắng nền (opaque), chữ đen, cỡ ~20px
    # &HAA BB GG RR (ASS color). Dùng &H00FFFFFF& = trắng không alpha; &H00000000& = đen.
    # Nền trắng (box) dùng OutlineColour khi BorderStyle=3
    # &HAABBGGRR: AA=alpha (00 = đậm, FF = trong suốt)
   # Hộp nền màu vàng, chữ đen
    style = (
        "Fontsize=11,"
        "BorderStyle=3,"
        "PrimaryColour=&H000000&,"    # chữ đen
        "OutlineColour=&H00FFFF&,"  # 00 = không trong suốt # nền vàng 50% trong suốt
        "Outline=3,"                  # độ dày hộp nền
        "Shadow=0,"
        "MarginV=80"
    )




    # Đường dẫn an toàn cho libass
    srt_for_ff = safe_path_for_ffmpeg(SRT_PATH)

    # Áp filter subtitles – cần encode lại video (không thể vcodec=copy)
    subbed_v = in_v.video.filter_(
        "subtitles",
        filename=srt_for_ff,
        charenc="UTF-8",
        force_style=style
    )

    (
        ffmpeg
        .output(
            subbed_v,
            final_audio,
            OUTPUT_MP4,
            vcodec="libx264", crf=18, preset="medium", pix_fmt="yuv420p",
            acodec=AUDIO_CODEC, audio_bitrate=AUDIO_BITRATE,
            shortest=None, movflags="+faststart"
        )
        .overwrite_output()
        .run(quiet=False)
    )

    print(">> Hoàn tất! File xuất:", OUTPUT_MP4)

if __name__ == "__main__":
    main()