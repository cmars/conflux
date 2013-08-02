package pqptree

const CreateTable_PNode = `
CREATE TABLE IF NOT EXISTS %s_pnode (
node_key TEXT NOT NULL,
svalues bytea NOT NULL,
num_elements INTEGER NOT NULL DEFAULT 0,
child_keys INTEGER[],
--
PRIMARY KEY (node_key))`

const CreateTable_PElement = `
CREATE TABLE IF NOT EXISTS %s_pelement (
node_key TEXT NOT NULL,
element bytea NOT NULL,
--
PRIMARY KEY (element, pnode_uuid),
FOREIGN KEY (pnode_uuid) REFERENCES %s_pnode(uuid))`
