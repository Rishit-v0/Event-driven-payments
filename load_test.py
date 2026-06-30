import asyncio
import time
import httpx

ORDER_URL = "http://localhost:8001/orders"
N_ORDERS = 200          # total orders to fire
CONCURRENCY = 100        # simultaneous in-flight requests
POLL_INTERVAL = 0.25    # seconds between status checks
POLL_TIMEOUT = 60       # max seconds to wait for one order to resolve


async def submit_order(client, sem):
    body = {
        "customer_email": "load@test.com",
        "product_id": "widget-001",
        "quantity": 1,
        "amount_cents": 2500,
    }
    async with sem:
        t0 = time.perf_counter()
        r = await client.post(ORDER_URL, json=body, timeout=30)
        r.raise_for_status()
        return r.json()["order_id"], t0


async def wait_for_resolution(client, order_id, t0, sem):
    deadline = time.perf_counter() + POLL_TIMEOUT
    async with sem:
        while time.perf_counter() < deadline:
            r = await client.get(f"{ORDER_URL}/{order_id}", timeout=30)
            status = r.json().get("status")
            if status in ("CONFIRMED", "CANCELLED"):
                return status, time.perf_counter() - t0
            await asyncio.sleep(POLL_INTERVAL)
    return "TIMEOUT", time.perf_counter() - t0


async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        print(f"Submitting {N_ORDERS} orders (concurrency={CONCURRENCY})...")
        submit_start = time.perf_counter()
        submitted = await asyncio.gather(
            *[submit_order(client, sem) for _ in range(N_ORDERS)]
        )
        submit_elapsed = time.perf_counter() - submit_start
        throughput = N_ORDERS / submit_elapsed
        print(
            f"Submitted {N_ORDERS} orders in {submit_elapsed:.2f}s "
            f"= {throughput:.1f} orders/sec accepted"
        )

        print("Waiting for sagas to resolve...")
        poll_sem = asyncio.Semaphore(CONCURRENCY)
        results = await asyncio.gather(
            *[wait_for_resolution(client, oid, t0, poll_sem) for oid, t0 in submitted]
        )

    latencies = sorted(lat for status, lat in results if status != "TIMEOUT")
    statuses = [status for status, _ in results]

    def pct(p):
        if not latencies:
            return 0.0
        return latencies[min(len(latencies) - 1, int(len(latencies) * p))]

    print("\n--- Results ---")
    print(
        f"Confirmed: {statuses.count('CONFIRMED')}  "
        f"Cancelled: {statuses.count('CANCELLED')}  "
        f"TimedOut: {statuses.count('TIMEOUT')}"
    )
    print("Saga completion latency (creation to terminal status):")
    print(f"  p50: {pct(0.50):.2f}s")
    print(f"  p95: {pct(0.95):.2f}s")
    print(f"  max: {(max(latencies) if latencies else 0):.2f}s")
    print(f"Submission throughput: {throughput:.1f} orders/sec")

if __name__ == "__main__":
    asyncio.run(main())