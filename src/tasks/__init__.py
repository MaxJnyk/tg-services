"""
Taskiq task definitions для tad-worker.

Все задачи регистрируются на broker из src.entrypoints.broker.
Каждый файл соответствует одному domain area:
- posting.py — ротация постов (label: posting)
- stats.py — сбор статистики каналов (label: stats)
- billing.py — биллинг: hold, charge, release (label: billing)
"""
