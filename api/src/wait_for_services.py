import socket
import time
import os

def wait_for(host, port, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(1)
    return False

if __name__ == "__main__":
    services = [
        ("postgres", 5432),
        ("kafka", 9092)
    ]

    for host, port in services:
        if not wait_for(host, port):
            raise TimeoutError(f"Service {host}:{port} not available")
