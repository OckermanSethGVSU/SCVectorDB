from pathlib import Path

milvus_path = Path("configs/milvus.yaml")
worker_ip_path = Path("worker.ip")

# Read replacement value (strip to avoid accidental newlines)
replacement = worker_ip_path.read_text().strip()

# Read milvus config
text = milvus_path.read_text()

# Replace all occurrences
text = text.replace("<HNS0>", replacement)

# Write back in place
milvus_path.write_text(text)