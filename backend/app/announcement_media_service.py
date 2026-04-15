from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

ANNOUNCEMENT_ALLOWED_VIDEO_EXTENSIONS = {
    '.mov', '.mp4', '.m4v', '.avi', '.mkv', '.webm', '.ts', '.mpeg', '.mpg', '.3gp'
}


def is_supported_video_filename(filename: str) -> bool:
    return Path(filename or '').suffix.lower() in ANNOUNCEMENT_ALLOWED_VIDEO_EXTENSIONS


def _ffmpeg_exists() -> bool:
    return shutil.which('ffmpeg') is not None


def _ffprobe_exists() -> bool:
    return shutil.which('ffprobe') is not None


def _relative_upload_url(path: Path) -> str:
    return f"/static/uploads/{path.name}"


def _has_audio_stream(src_path: str) -> bool:
    if not _ffprobe_exists():
        return True
    try:
        res = subprocess.run(
            ['ffprobe', '-v', 'error', '-select_streams', 'a', '-show_entries', 'stream=index', '-of', 'json', src_path],
            check=False,
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            return True
        data = json.loads(res.stdout or '{}')
        return bool(data.get('streams'))
    except Exception:
        return True


def normalize_announcement_video(src_path: str, output_dir: str) -> dict[str, Any]:
    src = Path(src_path)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = src.stem
    out_name = f"{stem}_normalized.mp4"
    out_path = out_dir / out_name
    if not src.exists():
        return {'ok': False, 'normalized_path': '', 'normalized_url': '', 'error': '源视频不存在'}
    if not _ffmpeg_exists():
        return {'ok': False, 'normalized_path': '', 'normalized_url': '', 'error': '服务器缺少 ffmpeg'}

    has_audio = _has_audio_stream(str(src))
    cmd = [
        'ffmpeg', '-y', '-i', str(src),
    ]
    if not has_audio:
        cmd += ['-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100', '-shortest']
    cmd += [
        '-c:v', 'libx264',
        '-pix_fmt', 'yuv420p',
        '-preset', 'veryfast',
        '-movflags', '+faststart',
        '-c:a', 'aac',
        '-b:a', '128k',
        str(out_path),
    ]
    try:
        res = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if res.returncode != 0 or not out_path.exists():
            err = (res.stderr or res.stdout or 'ffmpeg 转换失败')[-1200:]
            return {'ok': False, 'normalized_path': '', 'normalized_url': '', 'error': err}
        return {
            'ok': True,
            'normalized_path': str(out_path),
            'normalized_url': _relative_upload_url(out_path),
            'error': '',
        }
    except Exception as e:
        return {'ok': False, 'normalized_path': '', 'normalized_url': '', 'error': str(e)}


def _normalize_media_items(raw: Any) -> list[dict[str, Any]]:
    parsed = raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw or '[]')
        except Exception:
            parsed = []
    if not isinstance(parsed, list):
        return []
    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(parsed, start=1):
        if not isinstance(row, dict):
            continue
        url = str(row.get('url') or row.get('normalized_url') or row.get('source_url') or '').strip()
        if not url:
            continue
        sort = row.get('sort')
        try:
            sort = int(sort)
        except Exception:
            sort = idx
        rows.append({
            'type': 'video',
            'url': url,
            'source_url': str(row.get('source_url') or url).strip(),
            'normalized_url': str(row.get('normalized_url') or url).strip(),
            'sort': sort,
            'enabled': bool(row.get('enabled', True)),
        })
    rows = [x for x in rows if x['enabled']]
    rows.sort(key=lambda x: (x.get('sort') or 9999, x.get('url') or ''))
    return rows


def build_media_cache_from_items(items: Any) -> list[dict[str, Any]]:
    rows = _normalize_media_items(items)
    cache: list[dict[str, Any]] = []
    for row in rows:
        cache.append({
            'sort': int(row.get('sort') or 0),
            'source_url': str(row.get('source_url') or row.get('url') or '').strip(),
            'normalized_url': str(row.get('normalized_url') or row.get('url') or '').strip(),
            'telegram_file_id': '',
            'telegram_unique_id': '',
            'status': 'ready' if str(row.get('normalized_url') or row.get('url') or '').strip() else 'pending',
            'error': '',
            'updated_at': datetime.utcnow().isoformat(),
        })
    return cache


def merge_media_cache(existing_raw: Any, items_raw: Any) -> list[dict[str, Any]]:
    existing = build_media_cache_from_items([])
    parsed = existing_raw
    if isinstance(existing_raw, str):
        try:
            parsed = json.loads(existing_raw or '[]')
        except Exception:
            parsed = []
    if isinstance(parsed, list):
        existing = [x for x in parsed if isinstance(x, dict)]
    item_rows = _normalize_media_items(items_raw)
    existing_map: dict[tuple[int, str], dict[str, Any]] = {}
    for row in existing:
        key = (int(row.get('sort') or 0), str(row.get('normalized_url') or row.get('source_url') or '').strip())
        existing_map[key] = row
    merged: list[dict[str, Any]] = []
    for row in item_rows:
        normalized_url = str(row.get('normalized_url') or row.get('url') or '').strip()
        source_url = str(row.get('source_url') or row.get('url') or '').strip()
        key = (int(row.get('sort') or 0), normalized_url)
        prev = existing_map.get(key, {})
        merged.append({
            'sort': int(row.get('sort') or 0),
            'source_url': source_url,
            'normalized_url': normalized_url,
            'telegram_file_id': str(prev.get('telegram_file_id') or '').strip(),
            'telegram_unique_id': str(prev.get('telegram_unique_id') or '').strip(),
            'status': str(prev.get('status') or ('ready' if normalized_url else 'pending')),
            'error': str(prev.get('error') or ''),
            'updated_at': str(prev.get('updated_at') or datetime.utcnow().isoformat()),
        })
    return merged


def pick_album_send_items(media_cache_raw: Any, media_items_raw: Any) -> list[dict[str, Any]]:
    parsed = media_cache_raw
    if isinstance(media_cache_raw, str):
        try:
            parsed = json.loads(media_cache_raw or '[]')
        except Exception:
            parsed = []
    rows: list[dict[str, Any]] = []
    if isinstance(parsed, list):
        for row in parsed:
            if not isinstance(row, dict):
                continue
            rows.append({
                'sort': int(row.get('sort') or 0),
                'telegram_file_id': str(row.get('telegram_file_id') or '').strip(),
                'normalized_url': str(row.get('normalized_url') or '').strip(),
                'source_url': str(row.get('source_url') or '').strip(),
                'status': str(row.get('status') or '').strip() or 'pending',
            })
    if not rows:
        rows = build_media_cache_from_items(media_items_raw)
    rows.sort(key=lambda x: (x.get('sort') or 9999, x.get('normalized_url') or x.get('source_url') or ''))
    return rows


def save_telegram_file_ids(existing_raw: Any, file_rows: list[dict[str, Any]]) -> str:
    parsed = existing_raw
    if isinstance(existing_raw, str):
        try:
            parsed = json.loads(existing_raw or '[]')
        except Exception:
            parsed = []
    rows = parsed if isinstance(parsed, list) else []
    id_map = {int(x.get('sort') or 0): x for x in file_rows if isinstance(x, dict)}
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sort = int(row.get('sort') or 0)
        payload = id_map.get(sort)
        if payload:
            row['telegram_file_id'] = str(payload.get('telegram_file_id') or '').strip()
            row['telegram_unique_id'] = str(payload.get('telegram_unique_id') or '').strip()
            row['status'] = 'ready'
            row['error'] = ''
            row['updated_at'] = datetime.utcnow().isoformat()
        result.append(row)
    return json.dumps(result, ensure_ascii=False)
