# Distributed Task Scheduler with Load Balancing

A task scheduler for a simulated cloud cluster, written in C, with a live browser demo. Clients send tasks to a load balancer, which dispatches each one to a cloud node. You can watch tasks travel to nodes in different regions, change the balancing strategy while it runs, and fail a node to see its queue move to the others.

## Features

- Three load-balancing strategies: round-robin, least-loaded, and weighted by node speed
- Nodes with different compute speeds and cloud regions, with network latency that depends on the region
- Per-node priority queues (priority 1 to 5, higher runs first, first-come-first-served within a priority)
- Fault tolerance: when a node fails, its queued tasks are re-sent to the remaining nodes
- Live metrics: throughput, average wait, load imbalance, per-node utilization and more
- A dependency-free C backend with a small built-in HTTP server and JSON API
- A single-file frontend that only draws what the backend reports
- A command-line version of the scheduler that uses pthreads

## Project structure

```
backend/backend.c        Scheduler simulation and HTTP/JSON server
frontend/frontend.html   Browser UI (cloud diagram, parameters, node table, log)
cli/scheduler.c          Standalone command-line version
```

## Requirements

- A C compiler (`gcc` or `clang`) and `make`
- Linux or macOS (uses POSIX sockets and pthreads)
- A web browser

## Quick start

```
make
make run
```

Then open http://localhost:8080.

Press Start to begin. Use the controls to change the strategy, node count, task size, arrival rate, network latency and simulation speed. Click a node (or use its Fail button) to simulate a failure, and Restore to bring it back.

To build without `make`:

```
gcc -O2 -pthread -o backend backend/backend.c
./backend
```

Run the backend from the repository root, or pass the frontend path as an argument: `./backend path/to/frontend.html`.

## Command-line version

```
make cli
./build/scheduler -s weighted -n 4 -t 30 -f
```

Options: `-s rr|least|weighted`, `-n` nodes (up to 16), `-t` tasks, `-f` inject a node failure mid-run.

## How it works

1. The simulation thread advances time in 25 ms steps (multiplied by the simulation speed). Tasks arrive at the configured rate with random priority and size.
2. For each task the load balancer picks a live node: the next one in turn (round-robin), the one with the fewest queued and running tasks (least-loaded), or the one with the lowest load divided by its speed (weighted).
3. The task travels to the node for a time set by the base latency and the node's region, then waits in the node's priority queue.
4. A node runs one task at a time. A task takes its size divided by the node's speed.
5. When a node fails, it finishes the task it is running, and every queued task is sent again through the load balancer.

The frontend polls `GET /api/state` about 16 times a second and renders the result.
- Saving run results to a file for comparing strategies

## License

MIT. See [LICENSE](LICENSE).
