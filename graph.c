#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>

/* A task is a pointer to a function:  void task(int time) The parameter
   specifies how long it takes a task to complete (ms)
 */
typedef void (*task_t)(int time);

/* Struct and type to define 'list nodes' */
struct lnode;
typedef struct lnode lnode_t;

/* Struct and type to define 'graph nodes' */
struct gnode;
typedef struct gnode gnode_t;

/* A list node has a pointer to the next list element and a pointer to a graph
   node.
 */
struct lnode
{
  lnode_t *next;
  gnode_t *gnode;
};

/* Dependency status of a graph node (topology and runtime). A graph node can
   be triggered when 'required == satisfied'
 */
typedef struct
{
  int required;  /* number of parents (constant); pre-requisites */
  int satisfied; /* number of parents that finished their tasks at runtime */
} deps_t;

/* Total number of nodes.
 */
int gnode_count = 0;

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

/* List node constructor.
 */
lnode_t *lnode_new(void)
{
  lnode_t *node = (lnode_t *)malloc(sizeof(lnode_t));
  if (node == NULL)
    exit(EXIT_FAILURE);
  node->next = NULL;
  node->gnode = NULL;
  return node;
}

/* Append two list nodes.
 */
void lnode_append(lnode_t *node, gnode_t *gnode)
{
  lnode_t *tmp = node;
  while (tmp->next != NULL)
    tmp = tmp->next;
  tmp->next = lnode_new();
  tmp->next->gnode = gnode;
}

/* Graph node constructor.
 */
gnode_t *gnode_new(char label)
{
  gnode_t *node = (gnode_t *)malloc(sizeof(gnode_t));
  if (node == NULL)
    exit(EXIT_FAILURE);

  gnode_count++;
  node->label = label;
  node->deps.required = 0;
  node->deps.satisfied = 0;
  node->task = NULL;
  node->children = NULL;
  node->parents = NULL;
  mtx_init(&node->mutex, mtx_plain);

  return node;
}

/* Link two graph nodes, parent --> child. Child node is an already existing
   node.
 */
void gnode_child(gnode_t *parent, gnode_t *child)
{
  if (parent->children == NULL)
  {
    parent->children = lnode_new();
    parent->children->gnode = child;
  }
  else
    lnode_append(parent->children, child);

  if (child->parents == NULL)
  {
    child->parents = lnode_new();
    child->parents->gnode = parent;
  }
  else
    lnode_append(child->parents, parent);
}

/* Link two graph nodes, parent --> child. Child node is created with the
   indicated label.
 */
gnode_t *gnode_child_new(gnode_t *parent, char label)
{
  gnode_t *child = gnode_new(label);
  gnode_child(parent, child);
  return child;
}

/* Get node with given label.
 */
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

/* Print nodes from given graph (implementation).
 */
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

/* Print nodes from given graph.
 */
void gnode_print(gnode_t *node)
{
  char *node_str = malloc(sizeof(char) * (gnode_count + 1));
  if (node_str == NULL)
    exit(EXIT_FAILURE);
  for (int i = 0; i <= gnode_count; i++)
    node_str[i] = 0;
  impl_gnode_print(node, node_str);
  free(node_str);
}

int main(void)
{
  gnode_t *graph, *node, *end;

  /* Initial node */
  graph = gnode_new('A');
  end = gnode_new('Z');

  /* A --> { a, b, c } */
  gnode_child_new(graph, 'a');
  gnode_child_new(graph, 'b');
  gnode_child_new(graph, 'c');

  /* a --> { 1, 2 } */
  node = gnode_get(graph, 'a');
  gnode_child_new(node, '1');
  gnode_child_new(node, '2');

  /* b --> { 2 } */
  node = gnode_get(graph, 'b');
  gnode_child(node, gnode_get(graph, '2'));

  /* c -> { 2, 3, 4 } */
  node = gnode_get(graph, 'c');
  gnode_child(node, gnode_get(graph, '2'));
  gnode_child_new(node, '3');
  gnode_child_new(node, '4');

  /* 1 --> { Z } */
  gnode_child(gnode_get(graph, '1'), end);

  /* 2 --> { x, Z } */
  node = gnode_get(graph, '2');
  gnode_child_new(node, 'x');
  gnode_child(node, end);

  /* { 3, 4 } --> x */
  node = gnode_get(graph, 'x');
  gnode_child(gnode_get(graph, '3'), node);
  gnode_child(gnode_get(graph, '4'), node);

  /* x --> { Z } */
  gnode_child(node, end);

  gnode_print(graph);

  exit(EXIT_SUCCESS);
}
