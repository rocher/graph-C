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
/* A task is a pointer to a function: void task(void). The function only
   simulates how long it takes a task to complete (ms).
   See below #LINK - Task generator
 */
typedef void (*task_t)(void);

/*ANCHOR - List nodes */
struct lnode;
typedef struct lnode lnode_t;

/*ANCHOR - Graph nodes */
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

/* SECTION - DAGs */
#pragma region
/*****************************************************************************
 *
 *               DIRECTED GRAPH DEFINITION AND MANAGEMENT
 *
 *****************************************************************************/

/* SECTION - Types */

/*ANCHOR - List node */
/* A list node has a pointer to the next list element and a pointer to a graph
   node.
 */
struct lnode
{
  lnode_t *next;
  gnode_t *gnode;
};

/*ANCHOR - Dependencies */
/* Dependency status of a graph node (topology and runtime). A graph node can
   be triggered when 'required == satisfied'
 */
typedef struct
{
  int required;  /* number of parents (constant); pre-requisites */
  int satisfied; /* number of parents that finished their tasks at runtime */
} deps_t;

/*ANCHOR - Graph node */
/* A graph node has a number of dependencies that must be satisfied before the
   task can be triggered, a list of nodes that depend on it and a list of
   parents (dependencies/pre-requisites). A mutex is required to guarantee
   sequential operations in case several runners operate on the node.
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

/* SECTION - Functions */

/* SECTION - Lists */

/*ANCHOR - Node constructor */
lnode_t *lnode_new(gnode_t *gnode)
{
  lnode_t *node = (lnode_t *)mcalloc(sizeof(lnode_t));
  if (node == NULL)
    exit(EXIT_FAILURE);
  node->next = NULL;
  node->gnode = gnode;
  return node;
}

/*ANCHOR - Append gnode_t */
void lnode_append(lnode_t *node, gnode_t *gnode)
{
  lnode_t *tmp = node;
  while (tmp->next != NULL)
    tmp = tmp->next;
  tmp->next = lnode_new(gnode);
}
/*!SECTION - Lists */

/* SECTION - Graphs */

/*ANCHOR - Node counter */
int gnode_count = 0;

/*ANCHOR - Node constructor */
gnode_t *gnode_new(char label, task_t task)
{
  gnode_t *node = (gnode_t *)mcalloc(sizeof(gnode_t));
  if (node == NULL)
    exit(EXIT_FAILURE);

  gnode_count++;
  node->label = label;
  node->deps.required = 0;
  node->deps.satisfied = 0;
  node->task = task;
  node->children = NULL;
  node->parents = NULL;
  if (mtx_init(&node->mutex, mtx_plain) != thrd_success)
    exit(EXIT_FAILURE);

  return node;
}

/*ANCHOR - Link two nodes */
/* Link two graph nodes, parent --> child. Child node is an already existing
   node.
 */
void gnode_child(gnode_t *parent, gnode_t *child)
{
  if (parent->children == NULL)
  {
    parent->children = lnode_new(child);
  }
  else
    lnode_append(parent->children, child);
  child->deps.required++;

  if (child->parents == NULL)
  {
    child->parents = lnode_new(parent);
  }
  else
    lnode_append(child->parents, parent);
}

/*ANCHOR - Link a new node */
/* Link two graph nodes, parent --> child. Child node is created with the
   indicated label.
 */
gnode_t *gnode_child_new(gnode_t *parent, char label, task_t task)
{
  gnode_t *child = gnode_new(label, task);
  gnode_child(parent, child);
  return child;
}

/*ANCHOR - Get node from label */
gnode_t *gnode_get(gnode_t *node, char label)
{
  if (node->label == label)
    return node;

  lnode_t *tmp = node->children;
  while (tmp != NULL)
  {
    if (tmp->gnode->label == label)
      return tmp->gnode;
    tmp = tmp->next;
  }

  tmp = node->children;
  while (tmp != NULL)
  {
    gnode_t *node = gnode_get(tmp->gnode, label);
    if (node != NULL)
      return node;
    tmp = tmp->next;
  }

  return NULL;
}

/*ANCHOR - Print graph (impl) */
void impl_gnode_print(gnode_t *node, char *node_str)
{
  lnode_t *l;

  if (strchr(node_str, node->label) == NULL)
  {
    int i = 0;
    while (node_str[i] != 0)
      i++;
    node_str[i] = node->label;

    printf("NODE %c -->", node->label);

    l = node->children;
    while (l != NULL)
    {
      printf(" %c", l->gnode->label);
      l = l->next;
    }
    printf("\n");
  }

  l = node->children;
  while (l != NULL)
  {
    impl_gnode_print(l->gnode, node_str);
    l = l->next;
  }
}

/*ANCHOR - Print graph */
void gnode_print(gnode_t *node)
{
  char *node_str = mcalloc(sizeof(char) * (gnode_count + 1));
  if (node_str == NULL)
    exit(EXIT_FAILURE);
  for (int i = 0; i <= gnode_count; i++)
    node_str[i] = 0;
  impl_gnode_print(node, node_str);
  free(node_str);
}
/*!SECTION - Graphs */
/*!SECTION - Functions */
/*!SECTION - DAGs */
#pragma endregion

/* SECTION - Tasks definition */
#pragma region
/*****************************************************************************
 *
 *                      TASKS DEFINITION AND MANAGEMENT
 *
 *****************************************************************************/

/*ANCHOR - Global graph */
/* All tasks operate on the global graph */
gnode_t *graph;

/* SECTION - Task queue management */

lnode_t *tasks_queue = NULL;
int tasks_queue_count = 0;
mtx_t tasks_queue_mtx;
cnd_t tasks_queue_cvar;

/*ANCHOR - Tasks queue get */
gnode_t *tasks_queue_get()
{
  /* must be called right after the wait, with the tasks_queue_mutex locked */
  lnode_t *lnode = tasks_queue;
  gnode_t *gnode = tasks_queue->gnode;
  tasks_queue = tasks_queue->next;
  tasks_queue_count--;
  free(lnode);
  return gnode;
}

/*ANCHOR - Task queue append */
void task_queue_append(gnode_t *gnode)
{
  lock(&tasks_queue_mtx);
  {
    if (tasks_queue == NULL)
      tasks_queue = lnode_new(gnode);
    else
      lnode_append(tasks_queue, gnode);
    tasks_queue_count++;
    broadcast(&tasks_queue_cvar);
  }
  unlock(&tasks_queue_mtx);
}
/*!SECTION - Task queue management */

/*ANCHOR - Task generator */
#define GENERATE_TASK(NAME, MS)                                    \
  void task_##NAME(void)                                           \
  {                                                                \
    printf("task_##NAME\n");                                       \
    struct timespec time = {.tv_sec = 0, .tv_nsec = MS * 1000000}; \
    thrd_sleep(&time, NULL);                                       \
  }

/*ANCHOR - Initial task (A) */
void task_A(void)
{
  printf("NEXT CYCLE");
}

/*ANCHOR - Final task */
void task_Z(void)
{
  /* loop over the graph */
  task_queue_append(graph);
}
/*!SECTION - Tasks definition */
#pragma endregion

/* SECTION - Runners */
#pragma region
/*****************************************************************************
 *
 *                    RUNNERS DEFINITION AND MANAGEMENT
 *
 *****************************************************************************/

/* SECTION - Types */

/*!SECTION - Types */

/* SECTION - Variables */

bool runners_active = true;

thrd_t *runners_pool;
int **runners_id;

/*!SECTION - Variables */

/* SECTION - Functions */

/*ANCHOR - Runner */
int runner(void *arg)
{
  int *id = (int *)arg;
  gnode_t *gnode;

  printf("runner %d started\n", *id);

  while (runners_active)
  {
    /* wait for new pending tasks */
    while (tasks_queue_count == 0)
      wait(&tasks_queue_cvar, &tasks_queue_mtx);

    /* get first pending task */
    gnode = tasks_queue_get();
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
          task_queue_append(child->gnode);
      }
      unlock(&child->gnode->mutex);
      child = child->next;
    }
  }
  return 0;
}

/*ANCHOR - Runners init pool */
void runners_init_pool(int runners_count)
{
  if (mtx_init(&tasks_queue_mtx, mtx_plain) != thrd_success)
    exit(EXIT_FAILURE);

  if (cnd_init(&tasks_queue_cvar) != thrd_success)
    exit(EXIT_FAILURE);

  lock(&tasks_queue_mtx);
  tasks_queue_count = 0;

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

/*ANCHOR - Runners start */
void runners_start(gnode_t *graph)
{
}

/*!SECTION - Functions */

/*!SECTION - Runners */
#pragma endregion

/* SECTION - Tasks & main function */
#pragma region
/*****************************************************************************
 *
 *                         TASKS AND MAIN FUNCTION
 *
 *****************************************************************************/

/*ANCHOR - Tasks instantiation */
GENERATE_TASK(a, 100);
GENERATE_TASK(b, 300);
GENERATE_TASK(c, 200);
GENERATE_TASK(1, 200);
GENERATE_TASK(2, 100);
GENERATE_TASK(3, 300);
GENERATE_TASK(4, 200);
GENERATE_TASK(x, 50);

/*SECTION - main */
int main(void)
{
  gnode_t *node, *end;

  /*ANCHOR - Graph creation */
  /* Initial node */
  graph = gnode_new('A', task_A);
  end = gnode_new('Z', task_Z);

  /* A --> { a, b, c } */
  gnode_child_new(graph, 'a', task_a);
  gnode_child_new(graph, 'b', task_b);
  gnode_child_new(graph, 'c', task_c);

  /* a --> { 1, 2 } */
  node = gnode_get(graph, 'a');
  gnode_child_new(node, '1', task_1);
  gnode_child_new(node, '2', task_2);

  /* b --> { 2 } */
  node = gnode_get(graph, 'b');
  gnode_child(node, gnode_get(graph, '2'));

  /* c -> { 2, 3, 4 } */
  node = gnode_get(graph, 'c');
  gnode_child(node, gnode_get(graph, '2'));
  gnode_child_new(node, '3', task_3);
  gnode_child_new(node, '4', task_4);

  /* 1 --> { Z } */
  gnode_child(gnode_get(graph, '1'), end);

  /* 2 --> { x, Z } */
  node = gnode_get(graph, '2');
  gnode_child_new(node, 'x', task_x);
  gnode_child(node, end);

  /* { 3, 4 } --> x */
  node = gnode_get(graph, 'x');
  gnode_child(gnode_get(graph, '3'), node);
  gnode_child(gnode_get(graph, '4'), node);

  /* x --> { Z } */
  gnode_child(node, end);

  gnode_print(graph);

  /*ANCHOR - Runner pool initialization */
  runners_init_pool(6);

  sleep(1.5);

  exit(EXIT_SUCCESS);
}
/*!SECTION - main */
/*!SECTION - Tasks & main function */
#pragma endregion
