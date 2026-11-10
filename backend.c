/*
 * Cloud Task Scheduler - BACKEND (C)
 *
 * Simulates a cluster of cloud nodes behind a load balancer and exposes the
 * live state as JSON over a tiny built-in HTTP server (no external libraries).
 *
 * Build:  gcc -O2 -pthread -o backend backend.c
 * Run:    ./backend [path/to/frontend.html]   then open http://localhost:8080
 *          (default frontend path: frontend/frontend.html, relative to the working directory)
 *
 * API (all responses are the full state as JSON):
 *   GET  /api/state
 *   POST /api/start | /api/pause | /api/reset | /api/burst
 *   POST /api/fail[?node=i]  | /api/recover?node=i
 *   POST /api/config?strategy=rr|least|weighted&nodes=3..6&size=0.5|1|2
 *                   &rate=0.2..5&latency=100..1500&speed=1..6
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <netinet/in.h>

#define PORT        8080
#define MAX_NODES   6
#define MAX_FLIGHTS 1024
#define LOG_N       8
#define TICK_MS     25

static const double SPEEDS[] = {1, 2, 0.5, 1.5, 1, 2};
static const double LATF[]   = {1, 1.6, 2.2, 1.3, 1.8, 2.4};
static const char  *REGION[] = {"us-east", "eu-west", "ap-south", "us-west", "eu-north", "ap-east"};
static const char *frontend_path = "frontend/frontend.html";
static const char  *SKEY[]   = {"rr", "least", "weighted"};
static const char  *SNAME[]  = {"Round-robin", "Least-loaded", "Weighted by speed"};

typedef struct Task {
    int id, prio;
    double dur, born, ready;
    struct Task *next;
} Task;

typedef struct {
    int id, alive, qn, done, got;
    double speed, lf, busy, rem;
    Task *q;           /* sorted by priority desc, FIFO inside a priority */
    Task *cur;
} Node;

typedef struct { int active, id, prio, node, retry; double sent, dur; } Flight;
typedef struct { char msg[112]; int bad; } LogEntry;

/* ---- global state (guarded by lock) ---- */
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static Node nodes[MAX_NODES];
static Flight flights[MAX_FLIGHTS];
static LogEntry logs[LOG_N];
static int n_nodes = 4, strategy = 2, running = 0, rr_next = 0;
static int next_id = 1, gen = 0, gdone = 0, started = 0, retried = 0, dropped = 0;
static double T = 0, acc = 0, wait_sum = 0;
static double rate = 1.5, size_mul = 1.0, latency = 400, sim_speed = 2;

static void say(int bad, const char *fmt, ...) {
    memmove(&logs[1], &logs[0], sizeof(LogEntry) * (LOG_N - 1));
    va_list ap; va_start(ap, fmt);
    vsnprintf(logs[0].msg, sizeof logs[0].msg, fmt, ap);
    va_end(ap);
    logs[0].bad = bad;
}

static double load_of(Node *n) {
    return (n->qn + (n->cur ? 1 : 0)) / (strategy == 2 ? n->speed : 1.0);
}

static int pick(void) {
    if (strategy == 0) {
        for (int i = 0; i < n_nodes; i++) {
            int c = (rr_next + i) % n_nodes;
            if (nodes[c].alive) { rr_next = (c + 1) % n_nodes; return c; }
        }
        return -1;
    }
    int best = -1; double bl = 1e18;
    for (int i = 0; i < n_nodes; i++) {
        if (!nodes[i].alive) continue;
        double l = load_of(&nodes[i]);
        if (l < bl) { bl = l; best = i; }
    }
    return best;
}

static void submit(Task *t, int retry) {
    int i = pick();
    if (i < 0) { dropped++; say(1, "No live nodes. Task %d dropped.", t->id); free(t); return; }
    Node *n = &nodes[i];
    double dur = latency * n->lf * (retry ? 0.75 : 1.0);
    t->ready = T + dur; n->got++;
    Task **pp = &n->q;
    while (*pp && (*pp)->prio >= t->prio) pp = &(*pp)->next;
    t->next = *pp; *pp = t; n->qn++;
    for (int k = 0; k < MAX_FLIGHTS; k++)
        if (!flights[k].active) {
            flights[k] = (Flight){1, t->id, t->prio, i, retry, T, dur};
            break;
        }
    say(retry, "Task %d (priority %d) sent to node %d%s.", t->id, t->prio, i, retry ? " again" : "");
}

static void spawn(void) {
    Task *t = calloc(1, sizeof *t);
    gen++;
    t->id = next_id++; t->prio = 1 + rand() % 5;
    t->dur = (700 + rand() % 1800) * size_mul; t->born = T;
    submit(t, 0);
}

static void step(double dt) {
    T += dt; acc += dt / 1000.0 * rate;
    while (acc >= 1) { acc -= 1; spawn(); }
    for (int k = 0; k < MAX_FLIGHTS; k++)
        if (flights[k].active && T >= flights[k].sent + flights[k].dur) flights[k].active = 0;
    for (int i = 0; i < n_nodes; i++) {
        Node *n = &nodes[i];
        if (n->cur) {
            n->rem -= dt * n->speed; n->busy += dt;
            if (n->rem <= 0) { n->done++; gdone++; free(n->cur); n->cur = NULL; }
        }
        if (!n->cur && n->alive) {
            for (Task **pp = &n->q; *pp; pp = &(*pp)->next)
                if ((*pp)->ready <= T) {
                    Task *t = *pp; *pp = t->next; t->next = NULL; n->qn--;
                    n->cur = t; n->rem = t->dur; started++; wait_sum += T - t->born;
                    break;
                }
        }
    }
}

static int live_count(void) {
    int c = 0;
    for (int i = 0; i < n_nodes; i++) c += nodes[i].alive;
    return c;
}

static void fail_node(int i) {
    Node *n = &nodes[i];
    if (i < 0 || i >= n_nodes || !n->alive) return;
    if (live_count() < 2) { say(1, "Keep at least one node alive."); return; }
    n->alive = 0;
    for (int k = 0; k < MAX_FLIGHTS; k++)
        if (flights[k].active && flights[k].node == i) flights[k].active = 0;
    Task *list = n->q; int moved = n->qn;
    n->q = NULL; n->qn = 0; retried += moved;
    say(1, "Node %d failed. Moving %d queued task%s to other nodes.", i, moved, moved == 1 ? "" : "s");
    while (list) { Task *t = list; list = t->next; t->next = NULL; submit(t, 1); }
}

static void recover_node(int i) {
    if (i < 0 || i >= n_nodes || nodes[i].alive) return;
    nodes[i].alive = 1; say(0, "Node %d is back online.", i);
}

static void free_tasks(void) {
    for (int i = 0; i < MAX_NODES; i++) {
        for (Task *t = nodes[i].q; t; ) { Task *x = t->next; free(t); t = x; }
        free(nodes[i].cur);
    }
}

static void reset_sim(int n) {
    free_tasks();
    memset(nodes, 0, sizeof nodes); memset(flights, 0, sizeof flights); memset(logs, 0, sizeof logs);
    n_nodes = n; running = 0; rr_next = 0; next_id = 1;
    gen = gdone = started = retried = dropped = 0; T = acc = wait_sum = 0;
    for (int i = 0; i < n; i++) { nodes[i].id = i; nodes[i].speed = SPEEDS[i]; nodes[i].lf = LATF[i]; nodes[i].alive = 1; }
    say(0, "Cluster ready with %d nodes.", n);
}

/* ---- simulation thread ---- */
static void *sim_thread(void *arg) {
    (void)arg;
    for (;;) {
        usleep(TICK_MS * 1000);
        pthread_mutex_lock(&lock);
        if (running) for (int k = 0; k < (int)sim_speed; k++) step(TICK_MS);
        pthread_mutex_unlock(&lock);
    }
    return NULL;
}

/* ---- JSON ---- */
typedef struct { char *p; size_t n, cap; } Buf;
static void bp(Buf *b, const char *fmt, ...) {
    va_list ap;
    for (;;) {
        va_start(ap, fmt);
        int w = vsnprintf(b->p + b->n, b->cap - b->n, fmt, ap);
        va_end(ap);
        if (w >= 0 && (size_t)w < b->cap - b->n) { b->n += w; return; }
        b->cap = b->cap * 2 + 1024; b->p = realloc(b->p, b->cap);
    }
}

static char *state_json(size_t *len) {
    Buf b = {malloc(4096), 0, 4096};
    bp(&b, "{\"running\":%d,\"strategy\":\"%s\",\"strategy_name\":\"%s\",\"nodes_total\":%d,"
           "\"rate\":%.2f,\"size\":%.1f,\"latency\":%.0f,\"speed\":%.0f,\"t\":%.1f,"
           "\"gen\":%d,\"done\":%d,\"started\":%d,\"wait_sum\":%.1f,\"retried\":%d,\"dropped\":%d,"
           "\"rr_next\":%d,\"nodes\":[",
       running, SKEY[strategy], SNAME[strategy], n_nodes, rate, size_mul, latency, sim_speed, T,
       gen, gdone, started, wait_sum, retried, dropped, rr_next);
    for (int i = 0; i < n_nodes; i++) {
        Node *n = &nodes[i];
        bp(&b, "%s{\"id\":%d,\"region\":\"%s\",\"speed\":%.1f,\"lat_factor\":%.1f,\"alive\":%d,"
               "\"util\":%.1f,\"load\":%.2f,\"got\":%d,\"done\":%d,\"qn\":%d,\"running\":",
           i ? "," : "", i, REGION[i], n->speed, n->lf, n->alive,
           T > 0 ? 100.0 * n->busy / T : 0.0, load_of(n), n->got, n->done, n->qn);
        if (n->cur) bp(&b, "{\"id\":%d,\"p\":%d,\"progress\":%.3f}", n->cur->id, n->cur->prio,
                       1.0 - (n->rem > 0 ? n->rem : 0) / n->cur->dur);
        else bp(&b, "null");
        bp(&b, ",\"queued\":[");
        int c = 0;
        for (Task *t = n->q; t && c < 12; t = t->next, c++)
            bp(&b, "%s{\"id\":%d,\"p\":%d}", c ? "," : "", t->id, t->prio);
        bp(&b, "]}");
    }
    bp(&b, "],\"flights\":[");
    int c = 0;
    for (int k = 0; k < MAX_FLIGHTS; k++) {
        Flight *f = &flights[k];
        if (!f->active) continue;
        double p = (T - f->sent) / f->dur; if (p > 1) p = 1; if (p < 0) p = 0;
        bp(&b, "%s{\"id\":%d,\"p\":%d,\"node\":%d,\"progress\":%.3f,\"retry\":%d}",
           c++ ? "," : "", f->id, f->prio, f->node, p, f->retry);
    }
    bp(&b, "],\"log\":[");
    for (int i = 0; i < LOG_N; i++) {
        if (!logs[i].msg[0]) break;
        bp(&b, "%s{\"m\":\"%s\",\"bad\":%d}", i ? "," : "", logs[i].msg, logs[i].bad);
    }
    bp(&b, "]}");
    *len = b.n;
    return b.p;
}

/* ---- HTTP ---- */
static void respond(int fd, int code, const char *ctype, const char *body, size_t len) {
    char h[320];
    int n = snprintf(h, sizeof h,
        "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\n"
        "Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Content-Type\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        code, code == 200 ? "OK" : code == 204 ? "No Content" : "Not Found", ctype, len);
    send(fd, h, n, MSG_NOSIGNAL);
    if (len) send(fd, body, len, MSG_NOSIGNAL);
}

static int get_param(const char *q, const char *key, char *out, size_t sz) {
    size_t kl = strlen(key);
    for (const char *p = q; p && *p; ) {
        if (!strncmp(p, key, kl) && p[kl] == '=') {
            const char *v = p + kl + 1; size_t i = 0;
            while (v[i] && v[i] != '&' && i + 1 < sz) { out[i] = v[i]; i++; }
            out[i] = 0; return 1;
        }
        p = strchr(p, '&'); if (p) p++;
    }
    return 0;
}

static void apply_config(const char *q) {
    char v[32];
    if (get_param(q, "nodes", v, sizeof v)) {
        int n = atoi(v);
        if (n >= 3 && n <= MAX_NODES && n != n_nodes) reset_sim(n);
    }
    if (get_param(q, "strategy", v, sizeof v))
        for (int s = 0; s < 3; s++)
            if (!strcmp(v, SKEY[s]) && s != strategy) { strategy = s; say(0, "Strategy changed to %s.", SNAME[s]); }
    if (get_param(q, "size", v, sizeof v)) { double d = atof(v); if (d >= 0.5 && d <= 2) size_mul = d; }
    if (get_param(q, "rate", v, sizeof v)) { double d = atof(v); if (d >= 0.2 && d <= 5) rate = d; }
    if (get_param(q, "latency", v, sizeof v)) { double d = atof(v); if (d >= 100 && d <= 1500) latency = d; }
    if (get_param(q, "speed", v, sizeof v)) { double d = atof(v); if (d >= 1 && d <= 6) sim_speed = (int)d; }
}

static void handle(int fd) {
    char req[4096];
    ssize_t r = recv(fd, req, sizeof req - 1, 0);
    if (r <= 0) return;
    req[r] = 0;
    char method[8], target[512];
    if (sscanf(req, "%7s %511s", method, target) != 2) return;
    char *query = strchr(target, '?');
    if (query) *query++ = 0; else query = "";

    if (!strcmp(method, "OPTIONS")) { respond(fd, 204, "text/plain", "", 0); return; }

    if (!strcmp(target, "/") || !strcmp(target, "/index.html")) {
        FILE *f = fopen(frontend_path, "rb");
        if (!f) { const char *m = "Frontend file not found. Run the backend from the repository root or pass the path as an argument."; respond(fd, 404, "text/plain", m, strlen(m)); return; }
        fseek(f, 0, SEEK_END); long sz = ftell(f); rewind(f);
        char *buf = malloc(sz + 1); size_t got = fread(buf, 1, sz, f); fclose(f); sz = (long)got;
        respond(fd, 200, "text/html; charset=utf-8", buf, sz); free(buf);
        return;
    }
    if (strncmp(target, "/api/", 5)) { respond(fd, 404, "text/plain", "Not found", 9); return; }

    const char *act = target + 5;
    char v[16];
    pthread_mutex_lock(&lock);
    if (!strcmp(act, "start")) running = 1;
    else if (!strcmp(act, "pause")) running = 0;
    else if (!strcmp(act, "reset")) reset_sim(n_nodes);
    else if (!strcmp(act, "burst")) for (int i = 0; i < 10; i++) spawn();
    else if (!strcmp(act, "config")) apply_config(query);
    else if (!strcmp(act, "fail")) {
        if (get_param(query, "node", v, sizeof v)) fail_node(atoi(v));
        else {
            int alive[MAX_NODES], c = 0;
            for (int i = 0; i < n_nodes; i++) if (nodes[i].alive) alive[c++] = i;
            if (c < 2) say(1, "Keep at least one node alive."); else fail_node(alive[rand() % c]);
        }
    }
    else if (!strcmp(act, "recover")) { if (get_param(query, "node", v, sizeof v)) recover_node(atoi(v)); }
    size_t len; char *json = state_json(&len);
    pthread_mutex_unlock(&lock);
    respond(fd, 200, "application/json", json, len);
    free(json);
}

int main(int argc, char **argv) {
    if (argc > 1) frontend_path = argv[1];
    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL));
    reset_sim(4);
    pthread_t th; pthread_create(&th, NULL, sim_thread, NULL); pthread_detach(th);

    int srv = socket(AF_INET, SOCK_STREAM, 0), one = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = htons(PORT);
    if (bind(srv, (struct sockaddr *)&a, sizeof a) < 0 || listen(srv, 32) < 0) { perror("bind/listen"); return 1; }
    printf("Cloud scheduler backend running at http://localhost:%d\n", PORT);

    for (;;) {
        int fd = accept(srv, NULL, NULL);
        if (fd < 0) continue;
        struct timeval tv = {2, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        handle(fd);
        close(fd);
    }
}
