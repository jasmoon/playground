
import subprocess
import json
import sys
import time
import urllib.request


platform_is_windows = sys.platform.startswith("win")

def run_command(cmd):
    try:
        # Safe execution of subprocess with timeout
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, timeout=30)
        return output.decode("utf-8")
    except subprocess.CalledProcessError as e:
        return e.output.decode("utf-8")
    except subprocess.TimeoutExpired:
        return "Command timed out"

def ping(host):
    """Ping a host once and parse latency."""
    count_arg = "c"
    if platform_is_windows:
        count_arg = "n"
    result = run_command(f"ping -{count_arg} 1 {host}")
    latency = None
    success = False

    if "Packets: Sent = 1" in result:
        if "Received = 1" in result:
            success = True
            try:
                # Parse "time=12.3 ms"
                latency = result.split("time=")[1].split(" ")[0]
            except:
                latency = None

    return {
        "host": host,
        "success": success,
        "latency_ms": latency,
        "raw": result,
    }


def traceroute(host):
    """Run traceroute and return hop list."""
    cmd = "traceroute -m"
    if platform_is_windows:
        cmd = "tracert -h"
    result = run_command(f"{cmd} 2 {host}")
    hops = []

    for line in result.split("\r\n"):
        line = line.strip()
        if line and line[0].isdigit():
            hops.append(line)

    return {
        "host": host,
        "hops": hops,
        "raw": result,
    }


def http_check(url):
    """Check HTTP/HTTPS reachability and measure latency."""
    start = time.time()
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            latency = (time.time() - start) * 1000
            return {
                "url": url,
                "status": response.status,
                "latency_ms": round(latency, 2),
                "success": True,
            }
    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e),
        }


def main():
    target_host = "8.8.8.8"
    target_url = "https://www.apple.com"

    data = {
        "ping": ping(target_host),
        "traceroute": traceroute(target_host),
        "http": http_check(target_url),
    }

    # JSON reporting
    # Useful for orchestration systems
    # Good for building monitoring agents
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
