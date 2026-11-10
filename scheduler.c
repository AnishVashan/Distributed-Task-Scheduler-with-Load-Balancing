/*
 * Distributed Task Scheduler with Load Balancing (simulation in C)
 *
 * Each "node" is a worker thread with its own priority queue. A central
 * scheduler assigns incoming tasks to nodes using a load-balancing strategy.
 * Supports node failure with automatic task redistribution.
 *
 * Build:  make            Run: ./scheduler [-s rr|least|weighted] [-n nodes]
 *                                          [-t tasks] [-f]
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

#define MAX_NODES 16

typedef enum { RR, LEAST_LOADED, WEIGHTED } Strategy;

typedef struct Task {
    int id;
    int priority;          /* higher = more urgent (1..5) */
    int duration_ms;       /* base cost on a speed-1.0 node */
    int retries;           /* times rescheduled after node failure */
    struct Task *next;
} Task;

typedef struct {
    int id;
    double speed;          /* relative capacity, e.g. 0.5, 1.0, 2.0 */
    int alive;
    int shutdown;
    Task *head;            /* queue sorted by priority (desc), FIFO within */
    int queued;
    int running;
    int completed;
    long busy_ms;
    pthread_mutex_t lock;
    pthread_cond_t cv;
    pthread_t thread;
} Node;

static Node nodes[MAX_NODES];
static int num_nodes = 4;
static Strategy strategy = LEAST_LOADED;
static int rr_next = 0;
static pthread_mutex_t sched_lock = PTHREAD_MUTEX_INITIALIZER;

static int total_tasks = 40, done_tasks = 0;
static pthread_mutex_t done_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t done_cv = PTHREAD_COND_INITIALIZER;

/* ---------- queue helpers (caller holds node lock) ---------- */
static void enqueue(Node *n, Task *t) {
    Task **pp = &n->head;
    while (*pp && (*pp)->priority >= t->priority) pp = &(*pp)->next;
    t->next = *pp;
    *pp = t;
    n->queued++;
}

static Task *dequeue(Node *n) {
    Task *t = n->head;
    if (t) { n->head = t->next; t->next = NULL; n->queued--; }
    return t;
}

/* ---------- load balancing ---------- */
static double node_load(Node *n) {
    int l;
    pthread_mutex_lock(&n->lock);
    l = n->queued + n->running;
    pthread_mutex_unlock(&n->lock);
    return (strategy == WEIGHTED) ? l / n->speed : (double)l;
}

/* Returns index of chosen live node, or -1 if none. */
static int pick_node(void) {
    int best = -1;
    pthread_mutex_lock(&sched_lock);
    if (strategy == RR) {
        for (int i = 0; i < num_nodes; i++) {
            int c = (rr_next + i) % num_nodes;
            if (nodes[c].alive) { best = c; rr_next = (c + 1) % num_nodes; break; }
        }
    } else {
        double best_load = 1e18;
        for (int i = 0; i < num_nodes; i++) {
            if (!nodes[i].alive) continue;
            double l = node_load(&nodes[i]);
            if (l < best_load) { best_load = l; best = i; }
        }
    }
    pthread_mutex_unlock(&sched_lock);
    return best;
}

static void submit(Task *t) {
    for (;;) {
        int idx = pick_node();
        if (idx < 0) { fprintf(stderr, "No live nodes! Task %d dropped\n", t->id); free(t); return; }
        Node *n = &nodes[idx];
        pthread_mutex_lock(&n->lock);
        if (!n->alive) { pthread_mutex_unlock(&n->lock); continue; } /* raced with failure */
        enqueue(n, t);
        printf("[sched] task %2d (prio %d, %3dms) -> node %d\n",
               t->id, t->priority, t->duration_ms, n->id);
        pthread_cond_signal(&n->cv);
        pthread_mutex_unlock(&n->lock);
        return;
    }
}

/* ---------- worker ---------- */
static void *worker(void *arg) {
    Node *n = arg;
    for (;;) {
        pthread_mutex_lock(&n->lock);
        while (!n->head && !n->shutdown && n->alive)
            pthread_cond_wait(&n->cv, &n->lock);
        if (n->shutdown || !n->alive) { pthread_mutex_unlock(&n->lock); break; }
        Task *t = dequeue(n);
        n->running++;
        pthread_mutex_unlock(&n->lock);

        int ms = (int)(t->duration_ms / n->speed);
        usleep(ms * 1000);

        pthread_mutex_lock(&n->lock);
        n->running--; n->completed++; n->busy_ms += ms;
        pthread_mutex_unlock(&n->lock);
        printf("[node %d] finished task %2d in %3dms\n", n->id, t->id, ms);
        free(t);

        pthread_mutex_lock(&done_lock);
        if (++done_tasks == total_tasks) pthread_cond_signal(&done_cv);
        pthread_mutex_unlock(&done_lock);
    }
    return NULL;
}

/* Simulate crash: mark dead, reclaim queued tasks, reschedule elsewhere. */
static void fail_node(int idx) {
    Node *n = &nodes[idx];
    pthread_mutex_lock(&n->lock);
    n->alive = 0;
    Task *list = n->head;
    n->head = NULL; n->queued = 0;
    pthread_cond_signal(&n->cv);
    pthread_mutex_unlock(&n->lock);
    printf("\n*** node %d FAILED - redistributing its queue ***\n\n", idx);
    while (list) {
        Task *t = list; list = list->next; t->next = NULL; t->retries++;
        submit(t);
    }
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    int inject_failure = 0, opt;
    while ((opt = getopt(argc, argv, "s:n:t:fh")) != -1) {
        switch (opt) {
        case 's':
            if (!strcmp(optarg, "rr")) strategy = RR;
            else if (!strcmp(optarg, "least")) strategy = LEAST_LOADED;
            else if (!strcmp(optarg, "weighted")) strategy = WEIGHTED;
            else { fprintf(stderr, "unknown strategy\n"); return 1; }
            break;
        case 'n': num_nodes = atoi(optarg); break;
        case 't': total_tasks = atoi(optarg); break;
        case 'f': inject_failure = 1; break;
        default:
            printf("Usage: %s [-s rr|least|weighted] [-n nodes<=%d] [-t tasks] [-f]\n",
                   argv[0], MAX_NODES);
            return opt == 'h' ? 0 : 1;
        }
    }
    if (num_nodes < 1 || num_nodes > MAX_NODES || total_tasks < 1) {
        fprintf(stderr, "invalid node/task count\n"); return 1;
    }
    srand((unsigned)time(NULL));
    setvbuf(stdout, NULL, _IOLBF, 0);

    static const char *names[] = {"round-robin", "least-loaded", "weighted"};
    printf("Strategy: %s | nodes: %d | tasks: %d | failure: %s\n\n",
           names[strategy], num_nodes, total_tasks, inject_failure ? "yes" : "no");

    /* heterogeneous cluster: speeds cycle through 1.0, 2.0, 0.5, 1.5 */
    static const double speeds[] = {1.0, 2.0, 0.5, 1.5};
    for (int i = 0; i < num_nodes; i++) {
        Node *n = &nodes[i];
        memset(n, 0, sizeof *n);
        n->id = i; n->speed = speeds[i % 4]; n->alive = 1;
        pthread_mutex_init(&n->lock, NULL);
        pthread_cond_init(&n->cv, NULL);
        pthread_create(&n->thread, NULL, worker, n);
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int i = 1; i <= total_tasks; i++) {
        Task *t = calloc(1, sizeof *t);
        t->id = i;
        t->priority = 1 + rand() % 5;
        t->duration_ms = 20 + rand() % 80;
        submit(t);
        usleep(5000);                       /* arrival gap */
        if (inject_failure && num_nodes > 1 && i == total_tasks / 2)
            fail_node(num_nodes > 2 ? 1 : 0);
    }

    pthread_mutex_lock(&done_lock);
    while (done_tasks < total_tasks) pthread_cond_wait(&done_cv, &done_lock);
    pthread_mutex_unlock(&done_lock);
    clock_gettime(CLOCK_MONOTONIC, &t1);

    for (int i = 0; i < num_nodes; i++) {
        pthread_mutex_lock(&nodes[i].lock);
        nodes[i].shutdown = 1;
        pthread_cond_signal(&nodes[i].cv);
        pthread_mutex_unlock(&nodes[i].lock);
        pthread_join(nodes[i].thread, NULL);
    }

    double wall = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;
    printf("\n=========== Summary ===========\n");
    printf("%-6s %-6s %-9s %-10s %-8s %s\n", "Node", "Speed", "Tasks", "Busy(ms)", "Util%", "State");
    for (int i = 0; i < num_nodes; i++) {
        Node *n = &nodes[i];
        printf("%-6d %-6.1f %-9d %-10ld %-8.1f %s\n", n->id, n->speed, n->completed,
               n->busy_ms, 100.0 * n->busy_ms / wall, n->alive ? "alive" : "FAILED");
    }
    printf("Total wall time: %.0f ms\n", wall);
    return 0;
}
