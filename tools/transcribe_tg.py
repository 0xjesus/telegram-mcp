#!/usr/bin/env python3
"""Transcriptor de notas de voz de telegram-mcp (misma receta que el de whatsapp-mcp):
vigila messages.db, transcribe con faster-whisper `small` int8 en CPU (2 hilos, nice 19)
cada voz descargada por el daemon y escribe "[Nota de voz] …" en messages.text."""
import os, sqlite3, sys, time, logging, datetime as dt
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from consent import allowed, predicate

STORE = os.path.expanduser(os.environ.get("TG_STORE", "~/.local/share/telegram-mcp"))
DB = os.path.join(STORE, "messages.db")
MODEL = os.environ.get("TG_WHISPER_MODEL", "small"); THREADS = int(os.environ.get("TG_WHISPER_THREADS", "2"))
POLL = int(os.environ.get("TG_POLL_SECONDS", "30")); BATCH = int(os.environ.get("TG_BATCH", "4")); PREFIX = "[Nota de voz] "
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout); log = logging.getLogger("tg-transcriber")

def db():
    c = sqlite3.connect(DB, timeout=30); c.row_factory = sqlite3.Row; c.execute("PRAGMA journal_mode=WAL"); return c

def pending(c):
    return c.execute(f"""SELECT m.chat_id, m.id, m.media_path FROM messages m LEFT JOIN transcripts t ON t.chat_id=m.chat_id AND t.id=m.id
        WHERE {predicate('m.chat_id')} AND m.media_type='voice' AND m.media_path IS NOT NULL AND (t.status IS NULL OR (t.status='error' AND t.attempts<3))
        ORDER BY m.date DESC LIMIT ?""", (BATCH,)).fetchall()

def main():
    model = None
    while True:
        try:
            c = db(); rows = pending(c); c.close()
            if rows and model is None:
                from faster_whisper import WhisperModel
                log.info("cargando modelo %s", MODEL); model = WhisperModel(MODEL, device="cpu", compute_type="int8", cpu_threads=THREADS)
            for r in rows:
                c = db(); now = dt.datetime.now(dt.timezone.utc).isoformat()
                try:
                    if not allowed(c,r["chat_id"]):
                        c.close();continue
                    if not os.path.exists(r["media_path"]): raise FileNotFoundError(r["media_path"])
                    segs, info = model.transcribe(r["media_path"], vad_filter=True, beam_size=1)
                    text = " ".join(s.text.strip() for s in segs).strip()
                    c.execute("BEGIN IMMEDIATE")
                    if not allowed(c,r["chat_id"]):
                        c.rollback();c.close();continue
                    c.execute("INSERT INTO transcripts(chat_id,id,text,status,attempts,updated_at) VALUES(?,?,?,?,1,?) ON CONFLICT(chat_id,id) DO UPDATE SET text=excluded.text,status='done',attempts=attempts+1,error=NULL,updated_at=excluded.updated_at", (r["chat_id"], r["id"], text, "done", now))
                    c.execute("UPDATE messages SET text=? WHERE chat_id=? AND id=?", (PREFIX + (text or "(inaudible)"), r["chat_id"], r["id"]))
                    log.info("ok %s/%s (%s, %.0fs): %s", r["chat_id"], r["id"], info.language, info.duration, text[:80])
                except Exception as e:
                    c.execute("INSERT INTO transcripts(chat_id,id,status,attempts,error,updated_at) VALUES(?,?,'error',1,?,?) ON CONFLICT(chat_id,id) DO UPDATE SET status='error',attempts=attempts+1,error=excluded.error,updated_at=excluded.updated_at", (r["chat_id"], r["id"], str(e)[:300], now))
                    log.warning("error %s/%s: %s", r["chat_id"], r["id"], e)
                c.commit(); c.close()
            time.sleep(POLL if not rows else 1)
        except Exception as e:
            log.exception("loop"); time.sleep(POLL)

if __name__ == "__main__":
    main()
