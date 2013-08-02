package pqptree

const CreateTable_PNode = `
CREATE TABLE IF NOT EXISTS {{.Namespace}}_pnode (
node_key TEXT NOT NULL,
svalues bytea NOT NULL,
num_elements INTEGER NOT NULL DEFAULT 0,
child_keys INTEGER[],
--
PRIMARY KEY (node_key))`

const CreateTable_PElement = `
CREATE TABLE IF NOT EXISTS {{.Namespace}}_pelement (
node_key TEXT NOT NULL,
element bytea NOT NULL,
--
PRIMARY KEY (node_key, element),
UNIQUE (element),
FOREIGN KEY (node_key) REFERENCES {{.Namespace}}_pnode(node_key))`
