const fs = require('fs');
const express = require('express');
const app = express();
const cors = require("cors");
const crypto = require('crypto');

const DB_FILENAME = 'users.jsonl';
const IDX_FILENAME = 'users.idx';

// ==========================================
// 1. B-TREE CLASSES (The New Engine) 🌳
// ==========================================

class BTreeNode {
  constructor(t, leaf = false) {
    this.t = t; // Minimum degree (defines range for number of keys)
    this.leaf = leaf; // Boolean: true if leaf, false otherwise
    this.keys = []; // Array of objects { id, pos }
    this.children = []; // Array of BTreeNode references
    this.size = 0; // Subtree size count (Start with keys length)
  }
}

class BTree {
  constructor(t = 3) { // Degree 3 means Max 5 keys per node
    this.root = null;
    this.t = t;
  }

  // --- TRAVERSAL (For Debugging) ---
  traverse() {
    if (this.root) this._traverse(this.root);
  }
  _traverse(node) {
    let i;
    for (i = 0; i < node.keys.length; i++) {
      if (!node.leaf) this._traverse(node.children[i]);
      // console.log(node.keys[i]); 
    }
    if (!node.leaf) this._traverse(node.children[i]);
  }

  // --- SEARCH ---
  findFilePosition(id) {
    return this.root ? this._search(this.root, id) : null;
  }

  _search(node, id) {
    let i = 0;
    // Find the first key greater than or equal to k
    while (i < node.keys.length && id > node.keys[i].id) {
      i++;
    }

    // Found the key?
    if (i < node.keys.length && node.keys[i].id === id) {
      return node.keys[i].pos;
    }

    // If leaf, key is not present
    if (node.leaf) return null;

    // Go to the appropriate child
    return this._search(node.children[i], id);
  }

  // --- INSERTION ---
  insert(id, pos) {
    if (!this.root) {
      this.root = new BTreeNode(this.t, true);
      this.root.keys.push({ id, pos });
      this.root.size = 1; // Update size
    } else {
      // If root is full, tree grows in height
      if (this.root.keys.length === 2 * this.t - 1) {
        const s = new BTreeNode(this.t, false);
        s.children.push(this.root);
        this._splitChild(s, 0);
        
        // Decide which of the two children is going to have new key
        let i = 0;
        if (s.keys[0].id < id) i++;
        this._insertNonFull(s.children[i], id, pos);
        
        this.root = s;
        this.updateNodeSize(this.root); // Update root size
      } else {
        this._insertNonFull(this.root, id, pos);
        this.updateNodeSize(this.root); // Update root size
      }
    }
  }

  _insertNonFull(node, id, pos) {
    let i = node.keys.length - 1;

    if (node.leaf) {
      // Find location to insert and shift keys
      while (i >= 0 && node.keys[i].id > id) {
        i--;
      }
      node.keys.splice(i + 1, 0, { id, pos });
      // Size update happens implicitly as array grows, 
      // but parent needs to know in recursive updates
    } else {
      // Find child to go down to
      while (i >= 0 && node.keys[i].id > id) {
        i--;
      }
      i++; // Child index

      // Check if child is full
      if (node.children[i].keys.length === 2 * this.t - 1) {
        this._splitChild(node, i);
        if (node.keys[i].id < id) i++;
      }
      this._insertNonFull(node.children[i], id, pos);
    }
    
    // Update size after insert returns
    this.updateNodeSize(node);
  }

  _splitChild(parent, i) {
    const t = this.t;
    const y = parent.children[i]; // Full child
    const z = new BTreeNode(t, y.leaf); // New sibling

    // Copy last (t-1) keys of y to z
    z.keys = y.keys.splice(t); // Takes form index t to end
    
    // If not leaf, copy last t children of y to z
    if (!y.leaf) {
      z.children = y.children.splice(t);
    }

    // Median key moves up to parent
    const medianKey = y.keys.pop(); // The key at index t-1 (now last)

    parent.children.splice(i + 1, 0, z);
    parent.keys.splice(i, 0, medianKey);

    // Update sizes after split
    this.updateNodeSize(y);
    this.updateNodeSize(z);
    this.updateNodeSize(parent);
  }

  // --- SIZE TRACKING (Optimized for B-Tree) ---
  updateNodeSize(node) {
    let count = node.keys.length; // Local keys count
    if (!node.leaf) {
      for (let child of node.children) {
        count += child.size; // Add size of all children
      }
    }
    node.size = count;
  }

  // --- PAGINATION (Seek Logic) ---
  // Find key at absolute index (0 to N)
  // Logic: Iterate through keys and children accumulating counts
  findNodeByIndex(node, index) {
    let currentIdx = 0; // Relative index in this node's scope

    for (let i = 0; i < node.keys.length; i++) {
      // 1. Check Left Child
      if (!node.leaf) {
        const childSize = node.children[i].size;
        if (index < childSize) {
          return this.findNodeByIndex(node.children[i], index);
        }
        index -= childSize; // Skip the child
      }

      // 2. Check Current Key
      if (index === 0) {
        return node.keys[i]; // Found it!
      }
      index--; // Skip the key itself
    }

    // 3. Check Rightmost Child
    if (!node.leaf) {
      return this.findNodeByIndex(node.children[node.children.length - 1], index);
    }

    return null;
  }

  getRange(offset, limit) {
    const result = [];
    // Efficiently seek to offset and collect 'limit' items
    // Note: Pure seeking loop is simple but for very large limits could be optimized.
    // Since limit is small (20), calling findNodeByIndex 20 times is O(20 * log N) -> Very Fast.
    
    for (let i = 0; i < limit; i++) {
      const item = this.findNodeByIndex(this.root, offset + i);
      if (item) {
        result.push({ id: item.id, pos: item.pos });
      } else {
        break; 
      }
    }
    return result;
  }

  // --- SERIALIZATION ---
  // B-Trees are harder to serialize recursively due to structure. 
  // We flatten it to a list for saving, then rebuild.
  toArray() {
    const res = [];
    if(this.root) this._collect(this.root, res);
    return res;
  }
  _collect(node, res) {
    let i;
    for (i = 0; i < node.keys.length; i++) {
      if (!node.leaf) this._collect(node.children[i], res);
      res.push(node.keys[i]);
    }
    if (!node.leaf) this._collect(node.children[i], res);
  }

  toTree(list) {
    this.root = null;
    // For fast bulk load, we should sort and build (Phase 9 stuff).
    // For now, simple insert is fine.
    for (const n of list) this.insert(n.id, n.pos);
  }
  
  // NOTE: B-Tree Deletion is complex. 
  // For this version, we will use a "Lazy Delete" or rebuild method for simplicity in code size.
  // Implementing full B-Tree delete (merge/borrow) adds ~100 lines.
  // Using simple 'rebuild' on delete for now or standard array filter for demonstration.
  delete(id) {
      // Simplest Hack for B-Tree Delete without 200 lines of Merge Code:
      // 1. Get all items
      // 2. Filter out item
      // 3. Rebuild Tree
      // (Production DBs implement "Merge/Borrow", but for your project deadline this is safer)
      const allItems = this.toArray();
      const filtered = allItems.filter(i => i.id !== id);
      this.root = null;
      for(const item of filtered) {
          this.insert(item.id, item.pos);
      }
  }
}

// ==========================================
// 2. DATABASE ENGINE
// ==========================================

class GigaDb {
  constructor() {
    this.indexTree = new BTree(3); // 🔥 Using B-Tree with Degree 3
  }

  init() {
    if (!fs.existsSync(DB_FILENAME)) {
      fs.writeFileSync(DB_FILENAME, '');
      console.log("📁 DB file created");
      this.seed(50000); 
    } else if (fs.existsSync(IDX_FILENAME)) {
      console.log("⚡ Loading index from disk...");
      const raw = fs.readFileSync(IDX_FILENAME, 'utf-8');
      this.indexTree.toTree(JSON.parse(raw));
    } else {
      console.log("🛠️ Rebuilding index...");
      this.rebuildIndex();
      this.saveIndex();
    }
  }

  async seed(count) {
    console.time("Seeding time");
    const stream = fs.createWriteStream(DB_FILENAME, { flags: 'a' });
    let currentPos = 0;
    if (fs.existsSync(DB_FILENAME)) {
        currentPos = fs.statSync(DB_FILENAME).size;
    }
    for (let i = 0; i < count; i++) {
      const uniqueId = crypto.randomUUID();
      const user = {
        id: uniqueId,
        name: `User${i}`,
        email: `user${i}@gmail.com`,
        createdAt: Date.now()
      };
      const data = JSON.stringify(user) + '\n';
      const len = Buffer.alloc(4);
      len.writeUInt32BE(data.length);
      const buf = Buffer.concat([len, Buffer.from(data)]);
      if (!stream.write(buf)) { await new Promise(r => stream.once("drain", r)); }
      
      this.indexTree.insert(uniqueId, currentPos);
      currentPos += buf.length; 
    }
    stream.end();
    console.timeEnd("Seeding time");
    this.saveIndex(); 
  }

  rebuildIndex() {
    const data = fs.readFileSync(DB_FILENAME, 'utf-8');
    const lines = data.split('\n');
    let pos = 0;
    for (const line of lines) {
      if (!line) continue;
      const match = line.match(/"id":"([^"]+)"/);
      if (match) {
        this.indexTree.insert(match[1], pos);
      }
      pos += Buffer.byteLength(line + '\n');
    }
  }

  saveIndex() {
    console.log("💾 Saving index");
    fs.writeFileSync(IDX_FILENAME, JSON.stringify(this.indexTree.toArray()));
  }

  findById(id) {
    const start = process.hrtime.bigint();
    const position = this.indexTree.findFilePosition(id);
    if (position === null || position === undefined) {
      const end = process.hrtime.bigint();
      return { data: null, time_ms: Number(end - start) / 1e6 };
    }
    const fd = fs.openSync(DB_FILENAME, "r");
    const lenBuf = Buffer.alloc(4);
    fs.readSync(fd, lenBuf, 0, 4, position);
    const size = lenBuf.readUInt32BE(0);
    const dataBuf = Buffer.alloc(size);
    fs.readSync(fd, dataBuf, 0, size, position + 4);
    fs.closeSync(fd);
    const end = process.hrtime.bigint();
    return { data: JSON.parse(dataBuf.toString("utf8")), time_ms: Number(end - start) / 1e6 };
  }

  findByPage(pageNumber) {
    const start = process.hrtime.bigint();
    pageNumber = parseInt(pageNumber) || 1;
    const limit = 20;
    const offset = (pageNumber - 1) * limit;
    
    const nodeDataList = this.indexTree.getRange(offset, limit);

    if (nodeDataList.length === 0) return { users: [], time_ms: 0 };

    const fd = fs.openSync(DB_FILENAME, 'r');
    const users = [];
    const lenBuf = Buffer.alloc(4);

    for (const node of nodeDataList) {
        fs.readSync(fd, lenBuf, 0, 4, node.pos);
        const size = lenBuf.readUInt32BE(0);
        const dataBuffer = Buffer.alloc(size);
        fs.readSync(fd, dataBuffer, 0, size, node.pos + 4);
        users.push(JSON.parse(dataBuffer));
    }
    fs.closeSync(fd);
    const end = process.hrtime.bigint();
    return { users: users, time_ms: Number(end - start) / 1e6 }
  }

  async insertUser(name, email) {
    const start = process.hrtime.bigint(); 
    const uniqueId = crypto.randomUUID();
    const user = { id: uniqueId, name: name, email: email, createdAt: Date.now() };
    
    const data = JSON.stringify(user) + '\n';
    const len = Buffer.alloc(4);
    len.writeUInt32BE(data.length);
    const buf = Buffer.concat([len, Buffer.from(data)]);
    
    let currentPos = 0;
    if (fs.existsSync(DB_FILENAME)) { currentPos = fs.statSync(DB_FILENAME).size; }
    fs.appendFileSync(DB_FILENAME, buf);
    
    this.indexTree.insert(uniqueId, currentPos);
    this.saveIndex(); 

    const end = process.hrtime.bigint();
    return { 
        user: user, 
        time_ms: Number(end - start) / 1e6 
    };
  } 

  deleteUser(id) {
    const start = process.hrtime.bigint();
    
    const exists = this.indexTree.findFilePosition(id);
    if (exists === null || exists === undefined) return { success: false, msg: "User not found" };

    this.indexTree.delete(id);
    this.saveIndex();

    const end = process.hrtime.bigint();
    return { success: true, msg: "User deleted from index", time_ms: Number(end - start) / 1e6 };
  }
}

const db = new GigaDb();
db.init();

app.use(cors());
app.use(express.json());

app.get("/users/:id", (req, res) => {
  try {
    const result = db.findById(req.params.id);
    if (!result.data) return res.status(404).json({ success: false, msg: "User not found" });
    res.json({ success: true, time_ms: result.time_ms, user: result.data });
  } catch (err) { res.status(500).json({ success: false, msg: err.message }); }
});

app.get("/users/page/:id", (req, res) => {
  try {
    const result = db.findByPage(req.params.id);
    res.json({ success: true, page: Number(req.params.id), users: result.users, time_taken: result.time_ms });
  } catch (err) { res.status(500).json({ success: false, msg: err.message }); }
});

app.post("/users", async (req, res) => {
  try {
    const { name, email } = req.body;
    if (!name || !email) return res.status(400).json({ success: false, msg: "Required fields missing" });
    const result = await db.insertUser(name, email);
    res.json({ success: true, user: result.user, time_taken: result.time_ms });
  } catch (err) { res.status(500).json({ success: false, msg: err.message }); }
});

app.delete("/users/:id", (req, res) => {
  try {
    const result = db.deleteUser(req.params.id);
    if (!result.success) return res.status(404).json(result);
    res.json(result);
  } catch (err) { res.status(500).json({ success: false, msg: err.message }); }
});

app.listen(7101, () => {
  console.log("🚀 High-Perf GigaDB (B-Tree Edition) running on port 7101");
});