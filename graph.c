/******************************************************************************
 *
 *  Scheduling a Directed Acyclic Graph of Tasks
 *  https://github.com/rocher/graph-C
 *
 *  Copyright (c) 2023 Francesc Rocher <francesc.rocher@gmail.com>
 *  SPDX-License-Identifier: MIT
 *
 *****************************************************************************/

#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>
#include <time.h>
#include <unistd.h>

/* SECTION - Overall settings */
#pragma region
/*****************************************************************************
 *
 *                             OVERALL SETTINGS
 *
 *****************************************************************************/

/*ANCHOR - graph: print */
/* Check the validity of the constructed graph */
#define PRINT_GRAPH false

/*ANCHOR - log: loops */
/* Mark the start and end of a loop */
#define LOG_LOOPS false

/*ANCHOR - log: runner lifecycle */
/* Show creation, activation an deactivation of runners */
#define LOG_RUNNER_LIFECYCLE false

/*ANCHOR - log: runner/task */
/* Show who's running which task */
#define LOG_RUNNER_TASK false

/*ANCHOR - log: exec trace */
/* Show the execution trace at the end of a loop */
#define LOG_EXEC_TRACE false

/*ANCHOR - tasks: jitter */
/* Add some jitter to the task duration (+/- random 10% of the duration) */
#define TASK_JITTER false

/*!SECTION - Overall settings */
#pragma endregion

/* SECTION - Prototypes */
#pragma region
/*****************************************************************************
 *
 *                               PROTOTYPES
 *
 *****************************************************************************/

/*ANCHOR - Task */
/* A task is a pointer to a function: void task(void). In this context, tasks
   only simulate how long it takes to complete (wait for some ms).

   See below #LINK - Task generator
 */
typedef void (*task_t)(void);

/*ANCHOR - List node */
/* In a graph, the list of nodes connected to another node. */
struct lnode;
typedef struct lnode lnode_t;

/*ANCHOR - Graph node */
/* A graph node corresponds to a task. */
struct gnode;
typedef struct gnode gnode_t;

/*!SECTION - Prototypes */
#pragma endregion

/* SECTION - Utility functions */
#pragma region
/*****************************************************************************
 *
 *                             UTILITY FUNCTIONS
 *
 *****************************************************************************/

/*ANCHOR - mcalloc */
void *mcalloc(size_t size)
{
  void *addr = calloc(1, size);
  if (addr == NULL)
  {
    fprintf(stderr, "Error in calloc\n");
    exit(EXIT_FAILURE);
  }
  return addr;
}

/*ANCHOR - mutex: init */
void mutex_init(mtx_t *mutex)
{
  if (mtx_init(mutex, mtx_plain) != thrd_success)
  {
    fprintf(stderr, "Error in mtx_init\n");
    exit(EXIT_FAILURE);
  }
}

/*ANCHOR - mutex: lock */
void lock(mtx_t *mutex)
{
  int result = mtx_lock(mutex);
  if (result != thrd_success)
  {
    fprintf(stderr, "Error in mtx_lock\n");
    exit(EXIT_FAILURE);
  }
}

/*ANCHOR - mutex: unlock */
void unlock(mtx_t *mutex)
{
  int result = mtx_unlock(mutex);
  if (result != thrd_success)
  {
    fprintf(stderr, "Error in mtx_lock\n");
    exit(EXIT_FAILURE);
  }
}

/*ANCHOR - cvar: init*/
void cvar_init(cnd_t *cvar)
{
  if (cnd_init(cvar) != thrd_success)
  {
    fprintf(stderr, "Error in cnd_init\n");
    exit(EXIT_FAILURE);
  }
}

/*ANCHOR - cvar: wait */
void wait(cnd_t *cvar, mtx_t *mutex)
{
  if (cnd_wait(cvar, mutex) != thrd_success)
  {
    fprintf(stderr, "Error in cnd_wait\n");
    exit(EXIT_FAILURE);
  }
}

/*ANCHOR - cvar: broadcast */
void broadcast(cnd_t *var)
{
  if (cnd_broadcast(var) != thrd_success)
  {
    fprintf(stderr, "Error in cnd_broadcast\n");
    exit(EXIT_FAILURE);
  }
}
/*!SECTION - Utility functions */
#pragma endregion

/* SECTION - List of nodes */
#pragma region
/*****************************************************************************
 *
 *                      LISTS DEFINITION AND MANAGEMENT
 *
 *****************************************************************************/

/* SECTION - Types */

/*ANCHOR - lnode: struct */
/* A list node has a pointer to the next list element and a pointer to a graph
   node.
 */
struct lnode
{
  lnode_t *next;
  gnode_t *gnode;
};
/*!SECTION - Types */

/* SECTION - Functions */

/*ANCHOR - lnode: constructor */
lnode_t *lnode_new(gnode_t *gnode)
{
  lnode_t *lnode = (lnode_t *)mcalloc(sizeof(lnode_t));

  lnode->next = NULL;
  lnode->gnode = gnode;

  return lnode;
}

/*ANCHOR - lnode: append graph node */
void lnode_append(lnode_t *lnode, gnode_t *gnode)
{
  lnode_t *tmp = lnode;

  while (tmp->next != NULL)
    tmp = tmp->next;
  tmp->next = lnode_new(gnode);
}
/*!SECTION - Functions */
/*!SECTION - List of nodes */
#pragma endregion

/* SECTION - Graph of tasks */
#pragma region
/*****************************************************************************
 *
 *               DIRECTED GRAPH DEFINITION AND MANAGEMENT
 *
 *****************************************************************************/

/* SECTION - Types */

/*ANCHOR - dependencies: struct */
/* Dependency status of a graph node (topology and runtime). A graph node can
   be triggered when dependencies 'required == satisfied'
 */
typedef struct
{
  int required;  /* number of parents (constant); pre-requisites */
  int satisfied; /* number of parents that finished their tasks at runtime */
} deps_t;

/*ANCHOR - gnode: struct */
/* A graph node has a number of dependencies that must be satisfied before the
   task can be triggered, a list of nodes that depend on it and a list of
   parents (dependencies/pre-requisites). Parents are required to traves the
   graph in revers to fins the critical path. A mutex is required to guarantee
   sequential operations when several runners operate on the node.
 */
struct gnode
{
  char label;
  deps_t deps;
  task_t task;
  lnode_t *children;
  lnode_t *parents;
  mtx_t mutex;
};
/*!SECTION - Types */

/* SECTION - Variables */

/*ANCHOR - graph: global var */
/* All tasks operate on the global graph. This variable hold a pointer to the
gnode labeled 'A'. */
gnode_t *graph;

/*ANCHOR - graph: size */
/* Total number of gnodes */
int graph_size = 0;

/*ANCHOR - graph: loops */
/* Total number of loops to run */
int graph_loops;

/*ANCHOR - graph: loop */
/* Current loop number */
int graph_loop;

/*!SECTION - Variables */

/* SECTION - Functions */

/*ANCHOR - gnode: constructor */
gnode_t *gnode_new(char label, task_t task)
{
  gnode_t *gnode = (gnode_t *)mcalloc(sizeof(gnode_t));

  graph_size++;
  gnode->label = label;
  gnode->deps.required = 0;
  gnode->deps.satisfied = 0;
  gnode->task = task;
  gnode->children = NULL;
  gnode->parents = NULL;
  mutex_init(&gnode->mutex);

  return gnode;
}

/*ANCHOR - gnode: add existing child */
/* Link two graph nodes, parent --> child. Child node is an already existing
   gnode.
 */
void gnode_child(gnode_t *parent, gnode_t *child)
{
  if (parent->children == NULL)
    parent->children = lnode_new(child);
  else
    lnode_append(parent->children, child);
  child->deps.required++;

  if (child->parents == NULL)
    child->parents = lnode_new(parent);
  else
    lnode_append(child->parents, parent);
}

/*ANCHOR - gnode: add new child */
/* Link two graph nodes, parent --> child. Child node is created with the
   indicated label.
 */
gnode_t *gnode_child_new(gnode_t *parent, char label, task_t task)
{
  gnode_t *child = gnode_new(label, task);

  gnode_child(parent, child);

  return child;
}

/*ANCHOR - gnode: get from label */
gnode_t *gnode_get(gnode_t *gnode, char label)
{
  if (gnode->label == label)
    return gnode;

  lnode_t *child = gnode->children;
  while (child != NULL)
  {
    if (child->gnode->label == label)
      return child->gnode;
    child = child->next;
  }

  child = gnode->children;
  while (child != NULL)
  {
    gnode_t *gnode = gnode_get(child->gnode, label);
    if (gnode != NULL)
      return gnode;
    child = child->next;
  }

  return NULL;
}

/*ANCHOR - gnode: print graph (impl) */
void impl_gnode_print(gnode_t *gnode, char *gnode_labels)
{
  lnode_t *child;

  if (strchr(gnode_labels, gnode->label) == NULL)
  {
    int i = 0;
    while (gnode_labels[i] != 0)
      i++;
    gnode_labels[i] = gnode->label;

    printf("  node %c -->", gnode->label);
    child = gnode->children;
    while (child != NULL)
    {
      printf(" %c", child->gnode->label);
      child = child->next;
    }
    printf("\n");
  }

  child = gnode->children;
  while (child != NULL)
  {
    impl_gnode_print(child->gnode, gnode_labels);
    child = child->next;
  }
}

/*ANCHOR - gnode: print graph */
void gnode_print(gnode_t *gnode)
{
  if (!PRINT_GRAPH)
    return;

  char *gnode_labels = mcalloc(sizeof(char) * (graph_size + 1));

  printf("graph:\n");
  impl_gnode_print(gnode, gnode_labels);
  free(gnode_labels);
}
/*!SECTION - Functions */
/*!SECTION - Graph of tasks */
#pragma endregion

/* SECTION - Queue of tasks */
#pragma region
/*****************************************************************************
 *
 *                   TASKS QUEUE DEFINITION AND MANAGEMENT
 *
 *****************************************************************************/

/* SECTION - Variables */

/*ANCHOR - task queue: global var */
lnode_t *tasks_queue = NULL;

/*ANCHOR - task queue: length */
int tasks_queue_length = 0;

/*ANCHOR - task queue: mutex */
mtx_t tasks_queue_mtx;

/*ANCHOR - task queue: cond var */
cnd_t tasks_queue_cvar;

/*!SECTION - Variables */

/* SECTION - Functions */

/*ANCHOR - tasks queue: init */
void tasks_queue_init()
{
  tasks_queue_length = 0;
  mutex_init(&tasks_queue_mtx);
  cvar_init(&tasks_queue_cvar);
}

/*ANCHOR - tasks queue: pop front */
gnode_t *task_queue_pop_front()
{
  /* must be called right after the wait on the tasks_queue_cvar, with the
  tasks_queue_mtx locked */
  lnode_t *lnode = tasks_queue;
  gnode_t *gnode = tasks_queue->gnode;

  tasks_queue = tasks_queue->next;
  tasks_queue_length--;
  free(lnode);

  return gnode;
}

/*ANCHOR - task queue: push back */
void task_queue_push_back(gnode_t *gnode)
{
  lock(&tasks_queue_mtx);
  {
    if (tasks_queue == NULL)
      tasks_queue = lnode_new(gnode);
    else
      lnode_append(tasks_queue, gnode);
    tasks_queue_length++;
  }
  unlock(&tasks_queue_mtx);
  broadcast(&tasks_queue_cvar);
}
/*!SECTION - Functions */
/*!SECTION - Queue os tasks */
#pragma endregion

/* SECTION - Execution time & trace */
#pragma region
/*****************************************************************************
 *
 *                        EXECUTION TIME AND TRACE
 *
 *****************************************************************************/

/* SECTION - Types */

/*ANCHOR - exec time: type */
/* The result of 'end - start' is the duration time of a graph loop */
typedef struct
{
  clock_t start;
  clock_t end;
} exec_time_t;

/*ANCHOR - exec time: samples */
/* Used to compute the duration time of each graph loop  */
exec_time_t *exec_time_samples;

/*!SECTION - Types */

/* SECTION - Variables */

/*ANCHOR - exec trace: global var */
/* An execution trace is a sequence of characters (node labels) indicating the
start and end of a graph node. It is used to check the validity of a graph
loop, in which no child starts before all parents have finished. For example,
if 'A --> a', then a valid trace cannot contain '..A..a..A..'; the trace must
be like '..A..A..a..'. There is a trace per graph loop.
*/
char *exec_trace;

/*ANCHOR - exec trace: mutex */
mtx_t exec_trace_mtx;

/*!SECTION - Variables */

/* SECTION - Functions */

/*ANCHOR - exec trace: init */
/* Depends on the graph_size: must be called after the graph has been created.
 */
void exec_trace_init()
{
  exec_trace = mcalloc(sizeof(char) * (2 * graph_size + 1));
  mutex_init(&exec_trace_mtx);
}

void exec_trace_reset()
{
  exec_trace[0] = 0;
}

/*ANCHOR - exec trace: append */
void exec_trace_append(char label)
{
  int i = 0;
  lock(&exec_trace_mtx);
  {
    while (exec_trace[i] != 0)
      i++;
    exec_trace[i] = label;
    exec_trace[i + 1] = 0;
  }
  unlock(&exec_trace_mtx);
}

/*!SECTION - Functions */

/*!SECTION - Execution time & trace */
#pragma endregion

/* SECTION - Pool of runners */
#pragma region
/*****************************************************************************
 *
 *                    RUNNERS DEFINITION AND MANAGEMENT
 *
 *****************************************************************************/

/* SECTION - Variables */

/*ANCHOR - runners: active */
bool runners_active = true;

/*ANCHOR - runners: ids */
int **runners_id;

/*ANCHOR - runners: pool */
thrd_t *runners_pool;

/*ANCHOR - runners: pool_size */
int runners_pool_size;

/*ANCHOR - runners: count */
atomic_int runners_count;

/*!SECTION - Variables */

/* SECTION - Functions */

/*ANCHOR - runner: prototypes */
/* Check finalization conditions*/
void runner_check_loops();

/* Enqueue ready-to-run child nodes */
void runner_process_children(gnode_t *gnode);

/*ANCHOR - runner: implementation */
int runner(void *arg)
{
  int *id = (int *)arg;
  gnode_t *gnode;

  LOG_RUNNER_LIFECYCLE ? printf("runner %d start\n", *id) : 0;
  atomic_fetch_add(&runners_count, 1);

  while (runners_active)
  {
    /* wait for new pending tasks */
    lock(&tasks_queue_mtx);
    while (tasks_queue_length == 0)
      wait(&tasks_queue_cvar, &tasks_queue_mtx);

    if (!runners_active)
    {
      unlock(&tasks_queue_mtx);
      goto exit;
    }

    /* get first pending task */
    gnode = task_queue_pop_front();
    unlock(&tasks_queue_mtx);

    /* execute task */
    LOG_RUNNER_TASK ? printf("runner %d task %c\n", *id, gnode->label) : 0;
    exec_trace_append(gnode->label);
    (gnode->task)();
    exec_trace_append(gnode->label);

    /* reset satisfied dependencies for next loop */
    gnode->deps.satisfied = 0;

    if (gnode->label == 'Z')
      runner_check_loops();
    else
      runner_process_children(gnode);
  }

exit:
  LOG_RUNNER_LIFECYCLE ? printf("runner %d exit\n", *id) : 0;
  return 0;
}

/*ANCHOR - runner: check loops */
void runner_check_loops()
{
  LOG_EXEC_TRACE ? printf("exec trace: %s\n", exec_trace) : 0;
  if (graph_loop == graph_loops)
  {
    /* stop graph execution */
    printf("%d loops, stop runners\n", graph_loop);
    runners_active = false;
    tasks_queue_length = -1;
    broadcast(&tasks_queue_cvar);
  }
  else
  {
    /* loop over the graph */
    exec_trace_reset();
    task_queue_push_back(graph);
  }
}

/*ANCHOR - runner: process children */
void runner_process_children(gnode_t *gnode)
{
  /* update children dependencies; if met, append child to task queue */
  lnode_t *child = gnode->children;
  while (child != NULL)
  {
    lock(&child->gnode->mutex);
    {
      if (child->gnode->deps.required == ++child->gnode->deps.satisfied)
        task_queue_push_back(child->gnode);
    }
    unlock(&child->gnode->mutex);
    child = child->next;
  }
}

/*ANCHOR - runners: init pool */
void runners_init_pool(int size)
{
  runners_pool_size = size;
  runners_pool = mcalloc(sizeof(thrd_t) * runners_pool_size);
  runners_id = (int **)mcalloc(sizeof(int *) * runners_pool_size);
  atomic_init(&runners_count, 0);

  for (int i = 0; i < runners_pool_size; i++)
  {
    runners_id[i] = (int *)mcalloc(sizeof(int));
    *runners_id[i] = i;
    LOG_RUNNER_LIFECYCLE ? printf("runner %d create\n", i) : 0;
    if (thrd_create(&runners_pool[i], &runner, (void *)runners_id[i]) != thrd_success)
      exit(EXIT_FAILURE);
  }

  while (atomic_load(&runners_count) != runners_pool_size)
    ;
}

/*ANCHOR - runners: loop */
/* Run the task graph the specified number of loops */
void runners_loop(int loops)
{
  graph_loops = loops;
  task_queue_push_back(graph);
}

/*ANCHOR - runners: join */
void runners_join(void)
{
  for (int i = 0; i < runners_pool_size; i++)
    thrd_join(runners_pool[i], NULL);
}

/*!SECTION - Functions */
/*!SECTION - Pool of runners */
#pragma endregion

/* SECTION - Tasks implementation */
#pragma region
/*****************************************************************************
 *
 *                            TASKS IMPLEMENTATION
 *
 *****************************************************************************/

/*ANCHOR - task: initial (A) */
void task_A(void)
{
  LOG_LOOPS ? printf("-- start of loop\n") : 0;
  graph_loop++;
}

/*ANCHOR - task: final (Z) */
void task_Z(void)
{
  LOG_LOOPS ? printf("-- end of loop %d\n", graph_loop) : 0;
}

/*ANCHOR - tasks: macro generator */
#define GENERATE_TASK(NAME, MS)                            \
  void task_##NAME(void)                                   \
  {                                                        \
    int nsec = MS * 1000000;                               \
    if (TASK_JITTER)                                       \
      nsec += (1 - rand() % 3) * (rand() % (nsec / 10));   \
    struct timespec time = {.tv_sec = 0, .tv_nsec = nsec}; \
    thrd_sleep(&time, NULL);                               \
  }

/*ANCHOR - tasks: instantiation */
GENERATE_TASK(a, 100);
GENERATE_TASK(b, 200);
GENERATE_TASK(c, 100);
GENERATE_TASK(1, 20);
GENERATE_TASK(2, 50);
GENERATE_TASK(3, 50);
GENERATE_TASK(4, 100);
GENERATE_TASK(i, 100);
GENERATE_TASK(j, 80);
GENERATE_TASK(k, 50);
GENERATE_TASK(x, 50);
GENERATE_TASK(y, 100);

/*!SECTION - Tasks implementation */
#pragma endregion

/*SECTION - Main function */
int main(void)
{
  /*ANCHOR - Loops and Runners */
  int loops = 10;
  int runners = 5;
  gnode_t *gnode, *end;

  srand(time(NULL));

  /*ANCHOR - Graph creation */
  /* Initial and final nodes */
  graph = gnode_new('A', task_A);
  end = gnode_new('Z', task_Z);

  /* A --> { a, b, c } */
  gnode_child_new(graph, 'a', task_a);
  gnode_child_new(graph, 'b', task_b);
  gnode_child_new(graph, 'c', task_c);

  /* a --> { 1, 2 } */
  gnode = gnode_get(graph, 'a');
  gnode_child_new(gnode, '1', task_1);
  gnode_child_new(gnode, '2', task_2);

  /* b --> { 2 } */
  gnode = gnode_get(graph, 'b');
  gnode_child(gnode, gnode_get(graph, '2'));

  /* c -> { 3, 4 } */
  gnode = gnode_get(graph, 'c');
  gnode_child_new(gnode, '3', task_3);
  gnode_child_new(gnode, '4', task_4);

  /* 1 --> { i, j } */
  gnode = gnode_get(graph, '1');
  gnode_child_new(gnode, 'i', task_i);
  gnode_child_new(gnode, 'j', task_j);

  /* 2 --> { k } */
  gnode = gnode_get(graph, '2');
  gnode_child_new(gnode, 'k', task_k);

  /* 3 --> { k } */
  gnode = gnode_get(graph, '3');
  gnode_child(gnode, gnode_get(graph, 'k'));

  /* 4 --> { Z } */
  gnode = gnode_get(graph, '4');
  gnode_child(gnode, end);

  /* i --> { x } */
  gnode = gnode_get(graph, 'i');
  gnode_child_new(gnode, 'x', task_x);

  /* j --> { x, y } */
  gnode = gnode_get(graph, 'j');
  gnode_child(gnode, gnode_get(graph, 'x'));
  gnode_child_new(gnode, 'y', task_y);

  /* k --> { y } */
  gnode = gnode_get(graph, 'k');
  gnode_child(gnode, gnode_get(graph, 'y'));

  /* x --> { Z } */
  gnode = gnode_get(graph, 'x');
  gnode_child(gnode, end);

  /* y --> { Z } */
  gnode = gnode_get(graph, 'y');
  gnode_child(gnode, end);

  /* Print graph */
  gnode_print(graph);

  /*ANCHOR - Tasks queue init */
  tasks_queue_init();

  /*ANCHOR - Runners init */
  runners_init_pool(runners);

  /*ANCHOR - Execution trace init */
  exec_trace_init();

  /*ANCHOR - Runners start */
  runners_loop(loops);

  /*ANCHOR - Runners join */
  runners_join();

  /*TODO - Destroy all allocated resources */

  printf("exit %d\n", EXIT_SUCCESS);
  exit(EXIT_SUCCESS);
}
/*!SECTION - Main function */
