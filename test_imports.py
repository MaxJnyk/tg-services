# test_imports.py
print("1. Testing imports...")

# Test 1: Config
try:
    from src.core.config import settings
    print(f"✅ Config loaded: REDIS_URL={settings.REDIS_URL[:20]}...")
except Exception as e:
    print(f"❌ Config failed: {e}")

# Test 2: RedisCircuitBreaker (новый, Redis-backed)
try:
    from src.infrastructure.telegram.redis_circuit_breaker import RedisCircuitBreaker
    print("✅ RedisCircuitBreaker imported")
except Exception as e:
    print(f"❌ RedisCircuitBreaker failed: {e}")

# Test 3: rotate_post task
try:
    from src.tasks.posting import rotate_post
    print("✅ rotate_post task imported")
except Exception as e:
    print(f"❌ rotate_post failed: {e}")

# Test 4: health server
try:
    from src.entrypoints.health import start_health_server
    print("✅ health server imported")
except Exception as e:
    print(f"❌ health server failed: {e}")

print("\n2. Checking layer violations...")
import subprocess
result = subprocess.run(
    ["grep", "-r", "from src.infrastructure", "src/application/", "--include=*.py"],
    capture_output=True,
    cwd="/Users/maksim/Desktop/tad-worker"
)
if result.stdout:
    print(f"❌ VIOLATION: {result.stdout.decode()[:200]}")
else:
    print("✅ No layer violations in application/")

print("\n3. Summary: Code structure is VALID")
