const fs = require('fs');
const express = require('express');
const app = express();
const cors = require("cors");
const crypto = require('crypto');

const DB_FILENAME = 'users.jsonl';
const IDX_FILENAME = 'users.idx';

// ==========================================
// 1. ADVANCED AVL TREE (Order Statistic Tree)
// ==========================================

class Node {
  constructor(id, filePosition) {
    this.id = id;
    this.left = null;
    this.right = null;
    this.height = 1;
    this.filePosition = filePosition;
    this.size = 1; // Subtree size tracking
  }
}

class AvlIndexTree {
  constructor() {
    this.root = null;
  }

  getHeight(n) { return n ? n.height : 0; }
  getSize(n) { return n ? n.size : 0; }
  
  updateSize(n) {
    if (n) {
      n.size = 1 + this.getSize(n.left) + this.getSize(n.right);
    }
  }

  getBalance(n) { return n ? this.getHeight(n.left) - this.getHeight(n.right) : 0; }

  // --- Rotations (Updated with Size) ---
  rotateRight(y) {
    const x = y.left; const T2 = x.right;
    x.right = y; y.left = T2;
    
    y.height = Math.max(this.getHeight(y.left), this.getHeight(y.right)) + 1;
    x.height = Math.max(this.getHeight(x.left), this.getHeight(x.right)) + 1;
    
    this.updateSize(y);
    this.updateSize(x);
    
    return x;
  }

  rotateLeft(x) {
    const y = x.right; const T2 = y.left;
    y.left = x; x.right = T2;
    
    x.height = Math.max(this.getHeight(x.left), this.getHeight(x.right)) + 1;
    y.height = Math.max(this.getHeight(y.left), this.getHeight(y.right)) + 1;
    
    this.updateSize(x);
    this.updateSize(y);
    
    return y;
  }

  // --- Insertion ---
  insert(id, filePosition) {
    this.root = this._insert(this.root, id, filePosition);
  }

  _insert(node, id, filePosition) {
    if (!node) return new Node(id, filePosition);

    if (id < node.id) node.left = this._insert(node.left, id, filePosition);
    else if (id > node.id) node.right = this._insert(node.right, id, filePosition);
    else { node.filePosition = filePosition; return node; } // Update existing

    node.height = 1 + Math.max(this.getHeight(node.left), this.getHeight(node.right));
    this.updateSize(node);

    const balance = this.getBalance(node);

    if (balance > 1 && id < node.left.id) return this.rotateRight(node);
    if (balance < -1 && id > node.right.id) return this.rotateLeft(node);
    if (balance > 1 && id > node.left.id) { node.left = this.rotateLeft(node.left); return this.rotateRight(node); }
    if (balance < -1 && id < node.right.id) { node.right = this.rotateRight(node.right); return this.rotateLeft(node); }

    return node;
  }

  // --- 🔥 DELETION LOGIC ---
  delete(id) {
    this.root = this._delete(this.root, id);
  }

  getMinValueNode(node) {
    let current = node;
    while (current.left !== null) {
      current = current.left;
    }
    return current;
  }

  _delete(node, id) {
    if (!node) return node;

    if (id < node.id) {
      node.left = this._delete(node.left, id);
    } else if (id > node.id) {
      node.right = this._delete(node.right, id);
    } else {
      // Node Found
      if ((!node.left) || (!node.right)) {
        let temp = node.left ? node.left : node.right;
        if (!temp) {
          temp = node;
          node = null;
        } else {
          node = temp; 
        }
      } else {
        const temp = this.getMinValueNode(node.right);
        //console.log("Node")
        //console.log(temp.id);
        node.id = temp.id;
        node.filePosition = temp.filePosition;
        node.right = this._delete(node.right, temp.id);
      }
    }

    if (!node) return node;

    node.height = 1 + Math.max(this.getHeight(node.left), this.getHeight(node.right));
    this.updateSize(node);

    const balance = this.getBalance(node);

    if (balance > 1 && this.getBalance(node.left) >= 0) return this.rotateRight(node);
    if (balance > 1 && this.getBalance(node.left) < 0) { node.left = this.rotateLeft(node.left); return this.rotateRight(node); }
    if (balance < -1 && this.getBalance(node.right) <= 0) return this.rotateLeft(node);
    if (balance < -1 && this.getBalance(node.right) > 0) { node.right = this.rotateRight(node.right); return this.rotateLeft(node); }

    return node;
  }

  findFilePosition(id) {
    let current = this.root;
    while (current) {
      if (id === current.id) return current.filePosition;
      if (id < current.id) current = current.left;
      else current = current.right;
    }
    return null;
  }

  // O(log N) Lookup for Pagination
  findNodeByIndex(node, index) {
    if (!node) return null;
    const leftSize = this.getSize(node.left);
    if (index < leftSize) {
      return this.findNodeByIndex(node.left, index);
    } else if (index === leftSize) {
      return node;
    } else {
      return this.findNodeByIndex(node.right, index - leftSize - 1);
    }
  }

  getRange(offset, limit) {
    const result = [];
    for (let i = 0; i < limit; i++) {
      const node = this.findNodeByIndex(this.root, offset + i);
      if (node) {
        result.push({ id: node.id, pos: node.filePosition });
      } else {
        break;
      }
    }
    return result;
  }

  toArray() {
    const res = [];
    const stack = [];
    let cur = this.root;
    while (cur || stack.length) {
      while (cur) { stack.push(cur); cur = cur.left; }
      cur = stack.pop();
      res.push({ id: cur.id, filePosition: cur.filePosition });
      cur = cur.right;
    }
    return res;
  }

  toTree(list) {
    this.root = null;
    for (const n of list) this.insert(n.id, n.filePosition);
  }
}

// ==========================================
// 2. DATABASE ENGINE
// ==========================================

class GigaDb {
  constructor() {
    this.indexTree = new AvlIndexTree();
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
    if (position === null) {
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

  // 🔥 UPDATED: Measures Time Taken for Write Operation
  async insertUser(name, email) {
    const start = process.hrtime.bigint(); // ⏱️ Start Timer

    const uniqueId = crypto.randomUUID();
    const user = { id: uniqueId, name: name, email: email, createdAt: Date.now() };
    
    // 1. Buffer Create
    const data = JSON.stringify(user) + '\n';
    const len = Buffer.alloc(4);
    len.writeUInt32BE(data.length);
    const buf = Buffer.concat([len, Buffer.from(data)]);
    
    // 2. File Write
    let currentPos = 0;
    if (fs.existsSync(DB_FILENAME)) { currentPos = fs.statSync(DB_FILENAME).size; }
    fs.appendFileSync(DB_FILENAME, buf);
    
    // 3. Tree Insert
    this.indexTree.insert(uniqueId, currentPos);
    
    // 4. Save Index (Costly Operation - Tradeoff for durability)
    this.saveIndex(); 

    const end = process.hrtime.bigint(); // ⏱️ End Timer
    
    return { 
        user: user, 
        time_ms: Number(end - start) / 1e6 // Return time
    };
  } 

  deleteUser(id) {
    const start = process.hrtime.bigint();
    const exists = this.indexTree.findFilePosition(id);
    if (exists === null) return { success: false, msg: "User not found" };

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

// 🔥 UPDATED: Include time_taken in Response
app.post("/users", async (req, res) => {
  try {
    const { name, email } = req.body;
    if (!name || !email) return res.status(400).json({ success: false, msg: "Required fields missing" });
    
    const result = await db.insertUser(name, email);
    
    res.json({ 
        success: true, 
        user: result.user, 
        time_taken: result.time_ms // Sent to Frontend
    });
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
  console.log("🚀 High-Perf GigaDB running on port 7101");
});