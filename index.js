const fs = require('fs');
const express = require('express');
const app = express();
const cors = require("cors");
const crypto = require('crypto');

const DB_FILENAME = 'users.jsonl';
const IDX_FILENAME = 'users.idx';

// ==========================================
// 1. AVL TREE CLASSES
// ==========================================

class Node {
  constructor(id, filePosition) {
    this.id = id;
    this.left = null;
    this.right = null;
    this.height = 1;
    this.filePosition = filePosition;
  }
}

class AvlIndexTree {
  constructor() {
    this.root = null;
  }

  getHeight(n) { return n ? n.height : 0; }
  getBalance(n) { return n ? this.getHeight(n.left) - this.getHeight(n.right) : 0; }

  rotateRight(y) {
    const x = y.left; const T2 = x.right;
    x.right = y; y.left = T2;
    y.height = Math.max(this.getHeight(y.left), this.getHeight(y.right)) + 1;
    x.height = Math.max(this.getHeight(x.left), this.getHeight(x.right)) + 1;
    return x;
  }

  rotateLeft(x) {
    const y = x.right; const T2 = y.left;
    y.left = x; x.right = T2;
    x.height = Math.max(this.getHeight(x.left), this.getHeight(x.right)) + 1;
    y.height = Math.max(this.getHeight(y.left), this.getHeight(y.right)) + 1;
    return y;
  }

  insert(id, filePosition) {
    this.root = this._insert(this.root, id, filePosition);
  }

  _insert(node, id, filePosition) {
    if (!node) return new Node(id, filePosition);

    if (id < node.id) node.left = this._insert(node.left, id, filePosition);
    else if (id > node.id) node.right = this._insert(node.right, id, filePosition);
    else { node.filePosition = filePosition; return node; }

    node.height = 1 + Math.max(this.getHeight(node.left), this.getHeight(node.right));
    const balance = this.getBalance(node);

    if (balance > 1 && id < node.left.id) return this.rotateRight(node);
    if (balance < -1 && id > node.right.id) return this.rotateLeft(node);
    if (balance > 1 && id > node.left.id) { node.left = this.rotateLeft(node.left); return this.rotateRight(node); }
    if (balance < -1 && id < node.right.id) { node.right = this.rotateRight(node.right); return this.rotateLeft(node); }

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

  getRange(offset, limit) {
    const result = [];
    let count = 0;

    const traverse = (node) => {
      if (!node || result.length >= limit) return;
      
      traverse(node.left);

    
      if (result.length < limit) {
        if (count >= offset) {
          result.push({ id: node.id, pos: node.filePosition });
        }
        count++; 
      }

      if (result.length < limit) {
        traverse(node.right);
      }
    }

    traverse(this.root);
    return result;
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
      this.seed(1000000); 
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

      if (!stream.write(buf)) {
        await new Promise(r => stream.once("drain", r));
      }

      this.indexTree.insert(uniqueId, currentPos);
      
      currentPos += buf.length; 
    }

    stream.end();
    console.timeEnd("Seeding time");
    this.saveIndex(); 
  }

  rebuildIndex() {
    console.time("rebuilding index file")
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

    console.timeEnd("rebuilding index file");
  }

  saveIndex() {
    console.time("💾 Saving index");
    fs.writeFileSync(IDX_FILENAME, JSON.stringify(this.indexTree.toArray()));
    console.timeEnd("💾 Saving index");
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

    return {
      data: JSON.parse(dataBuf.toString("utf8")),
      time_ms: Number(end - start) / 1e6
    };
  }

  findByPage(pageNumber) {
    const start = process.hrtime.bigint();
    pageNumber = parseInt(pageNumber) || 1;

    const limit = 20; 
    const offset = (pageNumber - 1) * limit;

    const nodeDataList = this.indexTree.getRange(offset, limit);

    if (nodeDataList.length === 0) {
        return { users: [], time_ms: 0 };
    }

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

    return {
        users: users,
        time_ms: Number(end - start) / 1e6
    }
  }

  async insertUser(name, email) {
    const uniqueId = crypto.randomUUID();
    const user = {
      id: uniqueId,
      name: name,
      email: email,
      createdAt: Date.now()
    };

    const data = JSON.stringify(user) + '\n';
    const len = Buffer.alloc(4);
    len.writeUInt32BE(data.length);
    const buf = Buffer.concat([len, Buffer.from(data)]);

    let currentPos = 0;
    if (fs.existsSync(DB_FILENAME)) {
      currentPos = fs.statSync(DB_FILENAME).size;
    }

    fs.appendFileSync(DB_FILENAME, buf);
    this.indexTree.insert(uniqueId, currentPos);
    
    this.saveIndex(); 

    return user;
  }  
}

// ==========================================
// 3. EXPRESS SERVER
// ==========================================

const db = new GigaDb();
db.init();

app.use(cors());
app.use(express.json());

app.get("/users/:id", (req, res) => {
  try {
    const id = req.params.id; 
    const result = db.findById(id);

    if (!result.data) {
      return res.status(404).json({ success: false, msg: "User not found" });
    }

    res.json({ success: true, time_ms: result.time_ms, user: result.data });
  } catch (err) {
    res.status(500).json({ success: false, msg: err.message });
  }
});

app.get("/users/page/:id", (req, res) => {
  try {
    const page = req.params.id;
    const result = db.findByPage(page);
    
    res.json({
      success: true,
      page: Number(page),
      users: result.users,
      time_taken: result.time_ms
    });
  } catch (err) {
    res.status(500).json({ success: false, msg: err.message });
  }
});

app.post("/users", async (req, res) => {
  try {
    const { name, email } = req.body;
    if (!name || !email) {
      return res.status(400).json({ success: false, msg: "Name and Email required" });
    }
    
    const newUser = await db.insertUser(name, email);
    res.json({ success: true, user: newUser });
  } catch (err) {
    res.status(500).json({ success: false, msg: err.message });
  }
});

app.listen(7101, () => {
  console.log("🚀 DB Server running on port 7101");
});