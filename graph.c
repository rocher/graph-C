/******************************************************************************
 *
 *  Parallel Task Runner for Directed Acyclic Graphs
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
#include <unistd.h>

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

/*ANCHOR - MCalloc */
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

/*ANCHOR - Lock mutex */
void lock(mtx_t *mutex)
{
  int result = mtx_lock(mutex);
  if (result != thrd_success)
  {
    fprintf(stderr, "Error in mtx_lock\n");
    exit(EXIT_FAILURE);
  }
}

/*ANCHOR - Unlock mutex */
void unlock(mtx_t *mutex)
{
  int result = mtx_unlock(mutex);
  if (result != thrd_success)
  {
    fprintf(stderr, "Error in mtx_lock\n");
    exit(EXIT_FAILURE);
  }
}

/*ANCHOR - Cond var wait */
void wait(cnd_t *cvar, mtx_t *mutex)
{
  if (cnd_wait(cvar, mutex) != thrd_success)
  {
    fprintf(stderr, "Error in cnd_wait\n");
    exit(EXIT_FAILURE);
  }
}

/*ANCHOR - Cond var broadcast */
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

/*ANCHOR - gnode: global graph */
/* All tasks operate on the global graph */
gnode_t *graph;

/*ANCHOR - gnode: counter */
int gnode_count = 0;

/*!SECTION - Variables */

/* SECTION - Functions */

/*ANCHOR - gnode: constructor */
gnode_t *gnode_new(char label, task_t task)
{
  gnode_t *gnode = (gnode_t *)mcalloc(sizeof(gnode_t));

  gnode_count++;
  gnode->label = label;
  gnode->deps.required = 0;
  gnode->deps.satisfied = 0;
  gnode->task = task;
  gnode->children = NULL;
  gnode->parents = NULL;

  if (mtx_init(&gnode->mutex, mtx_plain) != thrd_success)
    exit(EXIT_FAILURE);

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

    printf("NODE %c -->", gnode->label);
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
  char *gnode_labels = mcalloc(sizeof(char) * (gnode_count + 1));

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

/*!SECTION - Variables */

/* SECTION - Functions */

/*ANCHOR - runners: implementation */
int runner(void *arg)
{
  int *id = (int *)arg;
  gnode_t *gnode;

  printf("runner %d started\n", *id);

  while (runners_active)
  {
    /* wait for new pending tasks */
    while (tasks_queue_length == 0)
      wait(&tasks_queue_cvar, &tasks_queue_mtx);

    /* get first pending task */
    gnode = task_queue_pop_front();
    unlock(&tasks_queue_mtx);

    /* execute task */
    (gnode->task)();

    /* reset satisfied dependencies for next cycle */
    gnode->deps.satisfied = 0;

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
  return 0;
}

/*ANCHOR - runners: init pool */
void runners_init_pool(int runners_count)
{
  if (mtx_init(&tasks_queue_mtx, mtx_plain) != thrd_success)
    exit(EXIT_FAILURE);

  if (cnd_init(&tasks_queue_cvar) != thrd_success)
    exit(EXIT_FAILURE);

  lock(&tasks_queue_mtx);
  tasks_queue_length = 0;

  runners_pool = mcalloc(sizeof(thrd_t) * runners_count);
  runners_id = (int **)mcalloc(sizeof(int *) * runners_count);

  for (int i = 0; i < runners_count; i++)
  {
    runners_id[i] = (int *)mcalloc(sizeof(int));
    *runners_id[i] = i;
    if (thrd_create(&runners_pool[i], &runner, (void *)runners_id[i]) != thrd_success)
      exit(EXIT_FAILURE);
    printf("runner %d created\n", i);
  }
}

/*ANCHOR - runners: start */
void runners_start(void)
{
  task_queue_push_back(graph);
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
  printf("NEXT CYCLE\n");
}

/*ANCHOR - task: final (Z) */
void task_Z(void)
{
  /* loop over the graph */
  task_queue_push_back(graph);
}

/*ANCHOR - tasks: macro generator */
#define GENERATE_TASK(NAME, MS)                                    \
  void task_##NAME(void)                                           \
  {                                                                \
    struct timespec time = {.tv_sec = 0, .tv_nsec = MS * 1000000}; \
    thrd_sleep(&time, NULL);                                       \
  }

/*ANCHOR - tasks: instantiation */
GENERATE_TASK(a, 100);
GENERATE_TASK(b, 300);
GENERATE_TASK(c, 200);
GENERATE_TASK(1, 200);
GENERATE_TASK(2, 100);
GENERATE_TASK(3, 300);
GENERATE_TASK(4, 200);
GENERATE_TASK(x, 50);
/*!SECTION - Tasks implementation */
#pragma endregion

/*SECTION - Main function */
int main(void)
{
  gnode_t *gnode, *end;

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

  /* c -> { 2, 3, 4 } */
  gnode = gnode_get(graph, 'c');
  gnode_child(gnode, gnode_get(graph, '2'));
  gnode_child_new(gnode, '3', task_3);
  gnode_child_new(gnode, '4', task_4);

  /* 1 --> { Z } */
  gnode_child(gnode_get(graph, '1'), end);

  /* 2 --> { x, Z } */
  gnode = gnode_get(graph, '2');
  gnode_child_new(gnode, 'x', task_x);
  gnode_child(gnode, end);

  /* { 3, 4 } --> x */
  gnode = gnode_get(graph, 'x');
  gnode_child(gnode_get(graph, '3'), gnode);
  gnode_child(gnode_get(graph, '4'), gnode);

  /* x --> { Z } */
  gnode_child(gnode, end);

  gnode_print(graph);

  /*ANCHOR - Runners initialization */
  runners_init_pool(6);

  /*ANCHOR - Runners start */
  runners_start();

  sleep(1);
  exit(EXIT_SUCCESS);
}
/*!SECTION - Main function */
